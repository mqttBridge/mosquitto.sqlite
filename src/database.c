/*
 * database.c — debug build: eviction tracing
 * Add -DEVICT_DEBUG to CFLAGS, or keep the #define below.
 * Remove this file (revert to original) once root cause is found.
 */

/* Uncomment to enable without changing CFLAGS */
/* #define EVICT_DEBUG */

#include "config.h"

#include <assert.h>
#include <stdio.h>
#include <utlist.h>

#include "mosquitto_broker_internal.h"
#include "send_mosq.h"
#include "sys_tree.h"
#include "util_mosq.h"

/* ------------------------------------------------------------------ */
/* Debug macro — logs at DEBUG level only when EVICT_DEBUG is defined  */
/* ------------------------------------------------------------------ */
#ifdef EVICT_DEBUG
#  define EVLOG(fmt, ...) \
     mosquitto_log_printf(MOSQ_LOG_DEBUG, "EVICT: " fmt, ##__VA_ARGS__)
#else
#  define EVLOG(fmt, ...) ((void)0)
#endif


bool db__ready_for_flight(struct mosquitto *context, enum mosquitto_msg_direction dir, int qos)
{
	struct mosquitto_msg_data *msgs;
	bool valid_bytes;
	bool valid_count;

	if(dir == mosq_md_out){
		msgs = &context->msgs_out;
	}else{
		msgs = &context->msgs_in;
	}

	if(msgs->inflight_maximum == 0 && db.config->max_inflight_bytes == 0){
		return true;
	}

	if(qos == 0){
		if(db.config->max_queued_messages == 0 && db.config->max_inflight_bytes == 0){
			return true;
		}
		valid_bytes = ((msgs->inflight_bytes - (ssize_t)db.config->max_inflight_bytes) < (ssize_t)db.config->max_queued_bytes);
		if(dir == mosq_md_out){
			valid_count = context->out_packet_count < db.config->max_queued_messages;
		}else{
			valid_count = msgs->inflight_count - msgs->inflight_maximum < db.config->max_queued_messages;
		}

		if(db.config->max_queued_messages == 0){
			return valid_bytes;
		}
		if(db.config->max_queued_bytes == 0){
			return valid_count;
		}
	}else{
		valid_bytes = (ssize_t)msgs->inflight_bytes12 < (ssize_t)db.config->max_inflight_bytes;
		valid_count = msgs->inflight_quota > 0;

		if(msgs->inflight_maximum == 0){
			return valid_bytes;
		}
		if(db.config->max_inflight_bytes == 0){
			return valid_count;
		}
	}

	return valid_bytes && valid_count;
}


bool db__ready_for_queue(struct mosquitto *context, int qos, struct mosquitto_msg_data *msg_data)
{
	int source_count;
	int adjust_count;
	long source_bytes;
	ssize_t adjust_bytes = (ssize_t)db.config->max_inflight_bytes;
	bool valid_bytes;
	bool valid_count;

	if(db.config->max_queued_messages == 0 && db.config->max_queued_bytes == 0){
		return true;
	}

	if(qos == 0 && db.config->queue_qos0_messages == false){
		return false;
	}else{
		source_bytes = (ssize_t)msg_data->queued_bytes12;
		source_count = msg_data->queued_count12;
	}
	adjust_count = msg_data->inflight_maximum;

	if(!net__is_connected(context)){
		adjust_bytes = 0;
		adjust_count = 0;
	}

	valid_bytes = (source_bytes - (ssize_t)adjust_bytes) < (ssize_t)db.config->max_queued_bytes;
	valid_count = source_count - adjust_count < db.config->max_queued_messages;

	if(db.config->max_queued_bytes == 0){
		return valid_count;
	}
	if(db.config->max_queued_messages == 0){
		return valid_bytes;
	}

	return valid_bytes && valid_count;
}


void db__msg_add_to_inflight_stats(struct mosquitto_msg_data *msg_data, struct mosquitto__client_msg *client_msg)
{
	msg_data->inflight_count++;
	msg_data->inflight_bytes += client_msg->base_msg->data.payloadlen;
	if(client_msg->data.qos != 0){
		msg_data->inflight_count12++;
		msg_data->inflight_bytes12 += client_msg->base_msg->data.payloadlen;
	}
	client_msg->base_msg->inflight_ref_count++;

	EVLOG("inflight_ref_count++ store_id=%llu now=%d",
		(unsigned long long)client_msg->base_msg->data.store_id,
		client_msg->base_msg->inflight_ref_count);
}


/* Forward declaration */
static void db__try_evict_payload(struct mosquitto__base_msg *base_msg);

static void db__msg_remove_from_inflight_stats(struct mosquitto_msg_data *msg_data, struct mosquitto__client_msg *client_msg)
{
	msg_data->inflight_count--;
	msg_data->inflight_bytes -= client_msg->base_msg->data.payloadlen;
	if(client_msg->data.qos != 0){
		msg_data->inflight_count12--;
		msg_data->inflight_bytes12 -= client_msg->base_msg->data.payloadlen;
	}
	if(client_msg->base_msg->inflight_ref_count > 0){
		client_msg->base_msg->inflight_ref_count--;
	}

	EVLOG("inflight_ref_count-- store_id=%llu now=%d",
		(unsigned long long)client_msg->base_msg->data.store_id,
		client_msg->base_msg->inflight_ref_count);

	/* Bug B fix: re-attempt eviction when last inflight subscriber ACKs */
	if(client_msg->base_msg->inflight_ref_count == 0){
		EVLOG("Bug-B path: last inflight gone, trying evict store_id=%llu",
			(unsigned long long)client_msg->base_msg->data.store_id);
		db__try_evict_payload(client_msg->base_msg);
	}
}


void db__msg_add_to_queued_stats(struct mosquitto_msg_data *msg_data, struct mosquitto__client_msg *client_msg)
{
	msg_data->queued_count++;
	msg_data->queued_bytes += client_msg->base_msg->data.payloadlen;
	if(client_msg->data.qos != 0){
		msg_data->queued_count12++;
		msg_data->queued_bytes12 += client_msg->base_msg->data.payloadlen;
	}
}


static void db__msg_remove_from_queued_stats(struct mosquitto_msg_data *msg_data, struct mosquitto__client_msg *client_msg)
{
	msg_data->queued_count--;
	msg_data->queued_bytes -= client_msg->base_msg->data.payloadlen;
	if(client_msg->data.qos != 0){
		msg_data->queued_count12--;
		msg_data->queued_bytes12 -= client_msg->base_msg->data.payloadlen;
	}
}


/*
 * db__try_evict_payload
 *
 * Frees the payload from RAM when:
 *   1. payload is not already NULL
 *   2. no subscriber currently has this message inflight
 *   3. the message has been durably written to SQLite (stored==true)
 *
 * Sets payload_on_disk=true so the broker reloads from SQLite before sending.
 */
static void db__try_evict_payload(struct mosquitto__base_msg *base_msg)
{
	/* Guard 0: null / already evicted */
	if(base_msg == NULL || base_msg->data.payload == NULL){
		EVLOG("store_id=%llu SKIP payload=NULL (already evicted or null msg)",
			base_msg ? (unsigned long long)base_msg->data.store_id : 0ULL);
		return;
	}

	/* Guard 1: still inflight to at least one subscriber */
	if(base_msg->inflight_ref_count > 0){
		EVLOG("store_id=%llu SKIP inflight_ref_count=%d (payload needed for delivery)",
			(unsigned long long)base_msg->data.store_id,
			base_msg->inflight_ref_count);
		return;
	}

	/* Guard 2: not yet written to SQLite — unsafe to evict */
	if(base_msg->stored == false){
		EVLOG("store_id=%llu SKIP stored=false (SQLite write not confirmed yet)",
			(unsigned long long)base_msg->data.store_id);
		return;
	}

	/* All guards passed — evict */
	EVLOG("store_id=%llu EVICTING payloadlen=%u inflight_ref=%d stored=%d",
		(unsigned long long)base_msg->data.store_id,
		base_msg->data.payloadlen,
		base_msg->inflight_ref_count,
		(int)base_msg->stored);

	mosquitto_FREE(base_msg->data.payload);
	base_msg->data.payload = NULL;
	base_msg->payload_on_disk = true;
}


int db__open(struct mosquitto__config *config)
{
	if(!config){
		return MOSQ_ERR_INVAL;
	}

	db.contexts_by_id = NULL;
	db.contexts_by_sock = NULL;
	db.contexts_for_free = NULL;
#ifdef WITH_BRIDGE
	db.bridges = NULL;
	db.bridge_count = 0;
#endif

	db.clientid_index_hash = NULL;
	db.normal_subs = NULL;
	db.shared_subs = NULL;

	sub__init();
	retain__init();

	db.config->security_options.unpwd = NULL;

#ifdef WITH_PERSISTENCE
	if(persist__restore()){
		return 1;
	}
#endif

	return MOSQ_ERR_SUCCESS;
}


static void subhier_clean(struct mosquitto__subhier **subhier)
{
	struct mosquitto__subhier *peer, *subhier_tmp;
	struct mosquitto__subleaf *leaf, *nextleaf;

	HASH_ITER(hh, *subhier, peer, subhier_tmp){
		leaf = peer->subs;
		while(leaf){
			nextleaf = leaf->next;
			mosquitto_FREE(leaf);
			leaf = nextleaf;
		}
		subhier_clean(&peer->children);
		HASH_DELETE(hh, *subhier, peer);
		mosquitto_FREE(peer);
	}
}


int db__close(void)
{
	subhier_clean(&db.normal_subs);
	subhier_clean(&db.shared_subs);
	retain__clean(&db.retains);
	db__msg_store_clean();
	return MOSQ_ERR_SUCCESS;
}


int db__msg_store_add(struct mosquitto__base_msg *base_msg)
{
	struct mosquitto__base_msg *found;
	unsigned hashv;

	HASH_VALUE(&base_msg->data.store_id, sizeof(base_msg->data.store_id), hashv);
	HASH_FIND_BYHASHVALUE(hh, db.msg_store, &base_msg->data.store_id, sizeof(base_msg->data.store_id), hashv, found);
	if(found == NULL){
		HASH_ADD_KEYPTR_BYHASHVALUE(hh, db.msg_store, &base_msg->data.store_id, sizeof(base_msg->data.store_id), hashv, base_msg);
		return MOSQ_ERR_SUCCESS;
	}else{
		return MOSQ_ERR_ALREADY_EXISTS;
	}
}


void db__msg_store_free(struct mosquitto__base_msg *base_msg)
{
	mosquitto_FREE(base_msg->data.source_id);
	mosquitto_FREE(base_msg->data.source_username);
	if(base_msg->dest_ids){
		for(int i=0; i<base_msg->dest_id_count; i++){
			mosquitto_FREE(base_msg->dest_ids[i]);
		}
		mosquitto_FREE(base_msg->dest_ids);
	}
	mosquitto_FREE(base_msg->data.topic);
	mosquitto_property_free_all(&base_msg->data.properties);
	mosquitto_FREE(base_msg->data.payload);
	mosquitto_FREE(base_msg);
}


void db__msg_store_remove(struct mosquitto__base_msg *base_msg, bool notify)
{
	if(base_msg == NULL){
		return;
	}
	HASH_DELETE(hh, db.msg_store, base_msg);
	db.msg_store_count--;
	db.msg_store_bytes -= base_msg->data.payloadlen;
	if(notify == true){
		plugin_persist__handle_base_msg_delete(base_msg);
	}
	db__msg_store_free(base_msg);
}


void db__msg_store_clean(void)
{
	struct mosquitto__base_msg *base_msg, *base_msg_tmp;

	HASH_ITER(hh, db.msg_store, base_msg, base_msg_tmp){
		db__msg_store_remove(base_msg, false);
	}
}


void db__msg_store_ref_inc(struct mosquitto__base_msg *base_msg)
{
	base_msg->ref_count++;
}


void db__msg_store_ref_dec(struct mosquitto__base_msg **base_msg)
{
	(*base_msg)->ref_count--;
	if((*base_msg)->ref_count == 0){
		db__msg_store_remove(*base_msg, true);
		*base_msg = NULL;
	}
}


void db__msg_store_compact(void)
{
	struct mosquitto__base_msg *base_msg, *base_msg_tmp;

	HASH_ITER(hh, db.msg_store, base_msg, base_msg_tmp){
		if(base_msg->ref_count < 1){
			db__msg_store_remove(base_msg, true);
		}
	}
}


static void db__message_remove_inflight(struct mosquitto *context, struct mosquitto_msg_data *msg_data, struct mosquitto__client_msg *item)
{
	if(!context || !msg_data || !item){
		return;
	}

	plugin_persist__handle_client_msg_delete(context, item);

	DL_DELETE(msg_data->inflight, item);
	if(item->base_msg){
		db__msg_remove_from_inflight_stats(msg_data, item);
		db__msg_store_ref_dec(&item->base_msg);
	}

	mosquitto_FREE(item);
}


static void db__message_remove_queued(struct mosquitto *context, struct mosquitto_msg_data *msg_data, struct mosquitto__client_msg *item)
{
	if(!context || !msg_data || !item){
		return;
	}

	plugin_persist__handle_client_msg_delete(context, item);

	DL_DELETE(msg_data->queued, item);
	if(item->base_msg){
		db__msg_remove_from_queued_stats(msg_data, item);
		db__msg_store_ref_dec(&item->base_msg);
	}

	mosquitto_FREE(item);
}


static void db__fill_inflight_out_from_queue(struct mosquitto *context)
{
	struct mosquitto__client_msg *client_msg, *tmp;

	DL_FOREACH_SAFE(context->msgs_out.queued, client_msg, tmp){
		if(!db__ready_for_flight(context, mosq_md_out, client_msg->data.qos)){
			return;
		}
		switch(client_msg->data.qos){
			case 0:
				client_msg->data.state = mosq_ms_publish_qos0;
				break;
			case 1:
				client_msg->data.state = mosq_ms_publish_qos1;
				break;
			case 2:
				client_msg->data.state = mosq_ms_publish_qos2;
				break;
		}
		if(client_msg->base_msg->data.expiry_time && db.now_real_s > client_msg->base_msg->data.expiry_time){
			db__message_remove_queued(context, &context->msgs_out, client_msg);
			continue;
		}
		/* Reload payload from SQLite if it was evicted from RAM while queued */
		if(client_msg->base_msg->payload_on_disk){
			EVLOG("reload from disk store_id=%llu client=%s",
				(unsigned long long)client_msg->base_msg->data.store_id,
				context->id ? context->id : "(null)");
			if(plugin_persist__handle_base_msg_load(client_msg->base_msg) != MOSQ_ERR_SUCCESS){
				mosquitto_log_printf(MOSQ_LOG_ERR,
					"EVICT: Failed to reload evicted payload store_id=%llu, dropping.",
					(unsigned long long)client_msg->base_msg->data.store_id);
				db__message_remove_queued(context, &context->msgs_out, client_msg);
				continue;
			}
		}
		plugin_persist__handle_client_msg_update(context, client_msg);
		db__message_dequeue_first(context, &context->msgs_out);
	}
}


void db__message_dequeue_first(struct mosquitto *context, struct mosquitto_msg_data *msg_data)
{
	struct mosquitto__client_msg *client_msg;

	UNUSED(context);

	client_msg = msg_data->queued;
	DL_DELETE(msg_data->queued, client_msg);
	DL_APPEND(msg_data->inflight, client_msg);
	if(msg_data->inflight_quota > 0){
		msg_data->inflight_quota--;
	}

	db__msg_remove_from_queued_stats(msg_data, client_msg);
	db__msg_add_to_inflight_stats(msg_data, client_msg);
}


int db__message_delete_outgoing(struct mosquitto *context, uint16_t mid, enum mosquitto_msg_state expect_state, int qos)
{
	struct mosquitto__client_msg *client_msg, *tmp;
	bool deleted = false;

	if(!context){
		return MOSQ_ERR_INVAL;
	}

	DL_FOREACH_SAFE(context->msgs_out.inflight, client_msg, tmp){
		if(client_msg->data.mid == mid){
			if(client_msg->data.qos != qos){
				mosquitto_log_printf(MOSQ_LOG_INFO, "Protocol error from %s: Mismatched QoS (%d:%d) when deleting outgoing message.",
						context->id, client_msg->data.qos, qos);
				return MOSQ_ERR_PROTOCOL;
			}else if(qos == 2 && client_msg->data.state != expect_state && expect_state != mosq_ms_any){
				mosquitto_log_printf(MOSQ_LOG_INFO, "Protocol error from %s: Mismatched state (%d:%d) when deleting outgoing message.",
						context->id, client_msg->data.state, expect_state);
				return MOSQ_ERR_PROTOCOL;
			}
			db__message_remove_inflight(context, &context->msgs_out, client_msg);
			deleted = true;
			break;
		}
	}

	if(deleted == false){
		DL_FOREACH_SAFE(context->msgs_out.queued, client_msg, tmp){
			if(client_msg->data.mid == mid){
				if(client_msg->data.qos != qos){
					return MOSQ_ERR_PROTOCOL;
				}else if(qos == 2 && client_msg->data.state != expect_state && expect_state != mosq_ms_any){
					return MOSQ_ERR_PROTOCOL;
				}
				db__message_remove_queued(context, &context->msgs_out, client_msg);
				break;
			}
		}
	}
	db__fill_inflight_out_from_queue(context);
#ifdef WITH_PERSISTENCE
	db.persistence_changes++;
#endif

	return db__message_write_inflight_out_latest(context);
}


int db__message_insert_incoming(struct mosquitto *context, uint64_t cmsg_id, struct mosquitto__base_msg *base_msg, bool persist)
{
	struct mosquitto__client_msg *client_msg;
	struct mosquitto_msg_data *msg_data;
	enum mosquitto_msg_state state = mosq_ms_invalid;
	int rc = 0;

	assert(base_msg);
	if(!context){
		return MOSQ_ERR_INVAL;
	}
	if(!context->id){
		return MOSQ_ERR_SUCCESS;
	}
	msg_data = &context->msgs_in;

	if(db__ready_for_flight(context, mosq_md_in, base_msg->data.qos)){
		state = mosq_ms_wait_for_pubrel;
	}else if(base_msg->data.qos != 0 && db__ready_for_queue(context, base_msg->data.qos, msg_data)){
		state = mosq_ms_queued;
		rc = 2;
	}else{
		if(context->is_dropping == false){
			context->is_dropping = true;
			mosquitto_log_printf(MOSQ_LOG_NOTICE,
					"Outgoing messages are being dropped for client %s.",
					context->id);
		}
		metrics__int_inc(mosq_counter_mqtt_publish_dropped, 1);
		context->stats.messages_dropped++;
		return 2;
	}

	assert(state != mosq_ms_invalid);

#ifdef WITH_PERSISTENCE
	if(state == mosq_ms_queued){
		db.persistence_changes++;
	}
#endif

	client_msg = mosquitto_malloc(sizeof(struct mosquitto__client_msg));
	if(!client_msg){
		return MOSQ_ERR_NOMEM;
	}
	client_msg->prev = NULL;
	client_msg->next = NULL;
	if(cmsg_id){
		client_msg->data.cmsg_id = cmsg_id;
	}else{
		client_msg->data.cmsg_id = ++context->last_cmsg_id;
	}
	client_msg->base_msg = base_msg;
	db__msg_store_ref_inc(client_msg->base_msg);
	client_msg->data.mid = base_msg->data.source_mid;
	client_msg->data.direction = mosq_md_in;
	client_msg->data.state = (uint8_t)state;
	client_msg->data.dup = false;
	if(base_msg->data.qos > context->max_qos){
		client_msg->data.qos = context->max_qos;
	}else{
		client_msg->data.qos = base_msg->data.qos;
	}
	client_msg->data.retain = base_msg->data.retain;
	client_msg->data.subscription_identifier = 0;

	if(state == mosq_ms_queued){
		DL_APPEND(msg_data->queued, client_msg);
		db__msg_add_to_queued_stats(msg_data, client_msg);
	}else{
		DL_APPEND(msg_data->inflight, client_msg);
		db__msg_add_to_inflight_stats(msg_data, client_msg);
	}

	if(persist && context->is_persisted){
		plugin_persist__handle_base_msg_add(client_msg->base_msg);
		plugin_persist__handle_client_msg_add(context, client_msg);
	}

	if(state == mosq_ms_queued){
		EVLOG("INSERT_INCOMING queued store_id=%llu client=%s persist=%d is_persisted=%d stored_before=%d",
			(unsigned long long)base_msg->data.store_id,
			context->id ? context->id : "(null)",
			(int)persist,
			(int)context->is_persisted,
			(int)base_msg->stored);

		/* Bug A fix: persist unconditionally for queued messages */
		plugin_persist__handle_base_msg_add(client_msg->base_msg);

		EVLOG("INSERT_INCOMING after handle_base_msg_add stored=%d payload=%s",
			(int)base_msg->stored,
			base_msg->data.payload ? "present" : "NULL");

		db__try_evict_payload(client_msg->base_msg);
	}

	if(client_msg->base_msg->data.qos > 0){
		util__decrement_receive_quota(context);
	}
	return rc;
}


int db__message_insert_outgoing(struct mosquitto *context, uint64_t cmsg_id, uint16_t mid, uint8_t qos, bool retain,
		struct mosquitto__base_msg *base_msg, uint32_t subscription_identifier, bool update, bool persist)
{
	struct mosquitto__client_msg *client_msg;
	struct mosquitto_msg_data *msg_data;
	enum mosquitto_msg_state state = mosq_ms_invalid;
	int rc = 0;
	char **dest_ids;

	assert(base_msg);
	if(!context){
		return MOSQ_ERR_INVAL;
	}
	if(!context->id){
		return MOSQ_ERR_SUCCESS;
	}
	context->stats.messages_sent++;

	msg_data = &context->msgs_out;

	if(context->protocol != mosq_p_mqtt5
			&& db.config->allow_duplicate_messages == false
			&& retain == false && base_msg->dest_ids){

		for(int i=0; i<base_msg->dest_id_count; i++){
			if(base_msg->dest_ids[i] && !strcmp(base_msg->dest_ids[i], context->id)){
				return MOSQ_ERR_SUCCESS;
			}
		}
	}

	if(!net__is_connected(context)){
		if(qos == 0 && !db.config->queue_qos0_messages){
			if(!context->bridge){
				return 2;
			}else{
				if(context->bridge->start_type != bst_lazy){
					return 2;
				}
			}
		}
		if(context->bridge && context->bridge->clean_start_local == true){
			return 2;
		}
	}

	if(net__is_connected(context)){
		if(db__ready_for_flight(context, mosq_md_out, qos)){
			switch(qos){
				case 0: state = mosq_ms_publish_qos0; break;
				case 1: state = mosq_ms_publish_qos1; break;
				case 2: state = mosq_ms_publish_qos2; break;
			}
		}else if(qos != 0 && db__ready_for_queue(context, qos, msg_data)){
			state = mosq_ms_queued;
			rc = 2;
		}else{
			if(context->is_dropping == false){
				context->is_dropping = true;
				mosquitto_log_printf(MOSQ_LOG_NOTICE,
						"Outgoing messages are being dropped for client %s.",
						context->id);
			}
			metrics__int_inc(mosq_counter_mqtt_publish_dropped, 1);
			return 2;
		}
	}else{
		if(db__ready_for_queue(context, qos, msg_data)){
			state = mosq_ms_queued;
		}else{
			metrics__int_inc(mosq_counter_mqtt_publish_dropped, 1);
			if(context->is_dropping == false){
				context->is_dropping = true;
				mosquitto_log_printf(MOSQ_LOG_NOTICE,
						"Outgoing messages are being dropped for client %s.",
						context->id);
			}
			return 2;
		}
	}
	assert(state != mosq_ms_invalid);

#ifdef WITH_PERSISTENCE
	if(state == mosq_ms_queued){
		db.persistence_changes++;
	}
#endif

	client_msg = mosquitto_malloc(sizeof(struct mosquitto__client_msg));
	if(!client_msg){
		return MOSQ_ERR_NOMEM;
	}
	client_msg->prev = NULL;
	client_msg->next = NULL;
	if(cmsg_id){
		client_msg->data.cmsg_id = cmsg_id;
	}else{
		client_msg->data.cmsg_id = ++context->last_cmsg_id;
	}
	client_msg->base_msg = base_msg;
	db__msg_store_ref_inc(client_msg->base_msg);
	client_msg->data.mid = mid;
	client_msg->data.direction = mosq_md_out;
	client_msg->data.state = (uint8_t)state;
	client_msg->data.dup = false;
	if(qos > context->max_qos){
		client_msg->data.qos = context->max_qos;
	}else{
		client_msg->data.qos = qos;
	}
	client_msg->data.retain = retain;
	client_msg->data.subscription_identifier = subscription_identifier;

	if(state == mosq_ms_queued){
		DL_APPEND(msg_data->queued, client_msg);
		db__msg_add_to_queued_stats(msg_data, client_msg);
	}else{
		DL_APPEND(msg_data->inflight, client_msg);
		db__msg_add_to_inflight_stats(msg_data, client_msg);
	}

	if(persist && context->is_persisted){
		plugin_persist__handle_base_msg_add(client_msg->base_msg);
		plugin_persist__handle_client_msg_add(context, client_msg);
	}

	if(state == mosq_ms_queued){
		EVLOG("INSERT_OUTGOING queued store_id=%llu client=%s persist=%d is_persisted=%d connected=%d stored_before=%d",
			(unsigned long long)base_msg->data.store_id,
			context->id ? context->id : "(null)",
			(int)persist,
			(int)context->is_persisted,
			(int)net__is_connected(context),
			(int)base_msg->stored);

		/* Bug A fix: persist unconditionally for queued messages */
		plugin_persist__handle_base_msg_add(client_msg->base_msg);

		EVLOG("INSERT_OUTGOING after handle_base_msg_add stored=%d payload=%s",
			(int)base_msg->stored,
			base_msg->data.payload ? "present" : "NULL");

		db__try_evict_payload(client_msg->base_msg);

		EVLOG("INSERT_OUTGOING after try_evict payload=%s payload_on_disk=%d",
			base_msg->data.payload ? "present" : "NULL",
			(int)base_msg->payload_on_disk);
	}

	if(db.config->allow_duplicate_messages == false && retain == false){
		dest_ids = mosquitto_realloc(base_msg->dest_ids, sizeof(char *)*(size_t)(base_msg->dest_id_count+1));
		if(dest_ids){
			base_msg->dest_ids = dest_ids;
			base_msg->dest_id_count++;
			base_msg->dest_ids[base_msg->dest_id_count-1] = mosquitto_strdup(context->id);
			if(!base_msg->dest_ids[base_msg->dest_id_count-1]){
				return MOSQ_ERR_NOMEM;
			}
		}else{
			return MOSQ_ERR_NOMEM;
		}
	}

#ifdef WITH_BRIDGE
	if(context->bridge && context->bridge->start_type == bst_lazy
			&& !net__is_connected(context)
			&& context->msgs_out.inflight_count + context->msgs_out.queued_count >= context->bridge->threshold){

		context->bridge->lazy_reconnect = true;
	}
#endif

	if(client_msg->data.qos > 0 && state != mosq_ms_queued){
		util__decrement_send_quota(context);
	}

	if(update){
		rc = db__message_write_inflight_out_latest(context);
		if(rc){
			return rc;
		}
		rc = db__message_write_queued_out(context);
		if(rc){
			return rc;
		}
	}

	return rc;
}


static inline int db__message_update_outgoing_state(struct mosquitto *context, struct mosquitto__client_msg *head,
		uint16_t mid, enum mosquitto_msg_state state, int qos, bool persist)
{
	struct mosquitto__client_msg *client_msg;

	DL_FOREACH(head, client_msg){
		if(client_msg->data.mid == mid){
			if(client_msg->data.qos != qos){
				mosquitto_log_printf(MOSQ_LOG_INFO, "Protocol error from %s: Mismatched QoS (%d:%d) when updating outgoing message.",
						context->id, client_msg->data.qos, qos);
				return MOSQ_ERR_PROTOCOL;
			}
			client_msg->data.state = (uint8_t)state;
			if(persist){
				plugin_persist__handle_client_msg_update(context, client_msg);
			}
			return MOSQ_ERR_SUCCESS;
		}
	}
	return MOSQ_ERR_NOT_FOUND;
}


int db__message_update_outgoing(struct mosquitto *context, uint16_t mid, enum mosquitto_msg_state state, int qos, bool persist)
{
	int rc;

	rc = db__message_update_outgoing_state(context, context->msgs_out.inflight, mid, state, qos, persist);
	if(!persist && rc == MOSQ_ERR_NOT_FOUND){
		rc = db__message_update_outgoing_state(context, context->msgs_out.queued, mid, state, qos, persist);
	}
	return rc;
}


static void db__messages_delete_list(struct mosquitto__client_msg **head)
{
	struct mosquitto__client_msg *client_msg, *tmp;

	DL_FOREACH_SAFE(*head, client_msg, tmp){
		DL_DELETE(*head, client_msg);
		db__msg_store_ref_dec(&client_msg->base_msg);
		mosquitto_FREE(client_msg);
	}
	*head = NULL;
}


int db__messages_delete_incoming(struct mosquitto *context)
{
	if(!context){
		return MOSQ_ERR_INVAL;
	}

	db__messages_delete_list(&context->msgs_in.inflight);
	db__messages_delete_list(&context->msgs_in.queued);
	context->msgs_in.inflight_bytes = 0;
	context->msgs_in.inflight_bytes12 = 0;
	context->msgs_in.inflight_count = 0;
	context->msgs_in.inflight_count12 = 0;
	context->msgs_in.queued_bytes = 0;
	context->msgs_in.queued_bytes12 = 0;
	context->msgs_in.queued_count = 0;
	context->msgs_in.queued_count12 = 0;

	return MOSQ_ERR_SUCCESS;
}


int db__messages_delete_outgoing(struct mosquitto *context)
{
	if(!context){
		return MOSQ_ERR_INVAL;
	}

	db__messages_delete_list(&context->msgs_out.inflight);
	db__messages_delete_list(&context->msgs_out.queued);
	context->msgs_out.inflight_bytes = 0;
	context->msgs_out.inflight_bytes12 = 0;
	context->msgs_out.inflight_count = 0;
	context->msgs_out.inflight_count12 = 0;
	context->msgs_out.queued_bytes = 0;
	context->msgs_out.queued_bytes12 = 0;
	context->msgs_out.queued_count = 0;
	context->msgs_out.queued_count12 = 0;

	return MOSQ_ERR_SUCCESS;
}


int db__messages_delete(struct mosquitto *context, bool force_free)
{
	if(!context){
		return MOSQ_ERR_INVAL;
	}

	if(force_free || context->clean_start || (context->bridge && context->bridge->clean_start)){
		db__messages_delete_incoming(context);
	}

	if(force_free || (context->bridge && context->bridge->clean_start_local)
			|| (context->bridge == NULL && context->clean_start)){

		db__messages_delete_outgoing(context);
	}

	return MOSQ_ERR_SUCCESS;
}


int db__messages_easy_queue(struct mosquitto *context, const char *topic, uint8_t qos, uint32_t payloadlen,
		const void *payload, int retain, uint32_t message_expiry_interval, mosquitto_property **properties)
{
	struct mosquitto__base_msg *base_msg;
	const char *source_id;
	enum mosquitto_msg_origin origin;

	if(!topic){
		return MOSQ_ERR_INVAL;
	}

	base_msg = mosquitto_calloc(1, sizeof(struct mosquitto__base_msg));
	if(base_msg == NULL){
		return MOSQ_ERR_NOMEM;
	}

	base_msg->data.topic = mosquitto_strdup(topic);
	if(base_msg->data.topic == NULL){
		db__msg_store_free(base_msg);
		return MOSQ_ERR_INVAL;
	}

	base_msg->data.qos = qos;
	if(db.config->retain_available == false){
		base_msg->data.retain = 0;
	}else{
		base_msg->data.retain = retain;
	}

	base_msg->data.payloadlen = payloadlen;
	if(payloadlen > 0){
		base_msg->data.payload = mosquitto_malloc(base_msg->data.payloadlen+1);
		if(base_msg->data.payload == NULL){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_NOMEM;
		}
		((uint8_t *)base_msg->data.payload)[base_msg->data.payloadlen] = 0;
		memcpy(base_msg->data.payload, payload, base_msg->data.payloadlen);
	}

	if(context && context->id){
		source_id = context->id;
	}else{
		source_id = "";
	}
	if(properties){
		base_msg->data.properties = *properties;
		*properties = NULL;
	}

	if(context){
		origin = mosq_mo_client;
	}else{
		origin = mosq_mo_broker;
	}
	if(db__message_store(context, base_msg, &message_expiry_interval, origin)){
		return 1;
	}

	return sub__messages_queue(source_id, base_msg->data.topic, base_msg->data.qos, base_msg->data.retain, &base_msg);
}


#define MOSQ_UUID_EPOCH 1637168273

uint64_t db__new_msg_id(void)
{
#ifdef WIN32
	FILETIME ftime;
	uint64_t ftime64;
#else
	struct timespec ts;
#endif
	uint64_t id;
	uint64_t tmp;
	time_t sec;
	long nsec;

	id = db.node_id_shifted;

#ifdef WIN32
	GetSystemTimePreciseAsFileTime(&ftime);
	ftime64 = (((uint64_t)ftime.dwHighDateTime)<<32) + ftime.dwLowDateTime;
	tmp = ftime64 - 116444736000000000LL;
	sec = tmp / 10000000;
	nsec = (long)(tmp - sec)*100;
#else
	clock_gettime(CLOCK_REALTIME, &ts);
	sec = ts.tv_sec;
	nsec = ts.tv_nsec;
#endif
	tmp = (sec - MOSQ_UUID_EPOCH) & 0x7FFFFFFF;
	id = id | (tmp << 23);

	tmp = (nsec & 0x7FFFFF80);
	id = id | (tmp >> 7);

	if(id <= db.last_db_id){
		id = db.last_db_id + 1;
	}
	db.last_db_id = id;

	return id;
}


int db__message_store(const struct mosquitto *source, struct mosquitto__base_msg *base_msg,
		uint32_t *message_expiry_interval, enum mosquitto_msg_origin origin)
{
	int rc;

	assert(base_msg);

	if(source && source->id){
		base_msg->data.source_id = mosquitto_strdup(source->id);
	}else{
		base_msg->data.source_id = mosquitto_strdup("");
	}
	if(!base_msg->data.source_id){
		mosquitto_log_printf(MOSQ_LOG_ERR, "Error: Out of memory.");
		db__msg_store_free(base_msg);
		return MOSQ_ERR_NOMEM;
	}

	if(source && source->username){
		base_msg->data.source_username = mosquitto_strdup(source->username);
		if(!base_msg->data.source_username){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_NOMEM;
		}
	}
	if(source){
		base_msg->source_listener = source->listener;
	}
	base_msg->origin = origin;
	if(message_expiry_interval){
		if(*message_expiry_interval > 0 && *message_expiry_interval != MSG_EXPIRY_INFINITE){
			base_msg->data.expiry_time = db.now_real_s + *message_expiry_interval;
		}else{
			base_msg->data.expiry_time = 0;
		}
	}

	base_msg->dest_ids = NULL;
	base_msg->dest_id_count = 0;
	db.msg_store_count++;
	db.msg_store_bytes += base_msg->data.payloadlen;

	if(!base_msg->data.store_id){
		base_msg->data.store_id = db__new_msg_id();
	}

	rc = db__msg_store_add(base_msg);
	if(rc){
		db__msg_store_free(base_msg);
		return rc;
	}

	return MOSQ_ERR_SUCCESS;
}


int db__message_store_find(struct mosquitto *context, uint16_t mid, struct mosquitto__client_msg **client_msg)
{
	struct mosquitto__client_msg *cmsg;

	*client_msg = NULL;

	if(!context){
		return MOSQ_ERR_INVAL;
	}

	DL_FOREACH(context->msgs_in.inflight, cmsg){
		if(cmsg->base_msg->data.source_mid == mid){
			*client_msg = cmsg;
			return MOSQ_ERR_SUCCESS;
		}
	}

	DL_FOREACH(context->msgs_in.queued, cmsg){
		if(cmsg->base_msg->data.source_mid == mid){
			*client_msg = cmsg;
			return MOSQ_ERR_SUCCESS;
		}
	}

	return 1;
}


static int db__message_reconnect_reset_outgoing(struct mosquitto *context)
{
	struct mosquitto__client_msg *client_msg, *tmp;

	context->msgs_out.inflight_bytes = 0;
	context->msgs_out.inflight_bytes12 = 0;
	context->msgs_out.inflight_count = 0;
	context->msgs_out.inflight_count12 = 0;
	context->msgs_out.queued_bytes = 0;
	context->msgs_out.queued_bytes12 = 0;
	context->msgs_out.queued_count = 0;
	context->msgs_out.queued_count12 = 0;
	context->msgs_out.inflight_quota = context->msgs_out.inflight_maximum;

	DL_FOREACH_SAFE(context->msgs_out.inflight, client_msg, tmp){
		db__msg_add_to_inflight_stats(&context->msgs_out, client_msg);
		if(client_msg->data.qos > 0){
			util__decrement_send_quota(context);
		}

		switch(client_msg->data.qos){
			case 0: client_msg->data.state = mosq_ms_publish_qos0; break;
			case 1: client_msg->data.state = mosq_ms_publish_qos1; break;
			case 2:
				if(client_msg->data.state == mosq_ms_wait_for_pubcomp){
					client_msg->data.state = mosq_ms_resend_pubrel;
				}else{
					client_msg->data.state = mosq_ms_publish_qos2;
				}
				break;
		}
		plugin_persist__handle_client_msg_update(context, client_msg);
	}
	DL_FOREACH_SAFE(context->msgs_out.queued, client_msg, tmp){
		db__msg_add_to_queued_stats(&context->msgs_out, client_msg);
	}
	db__fill_inflight_out_from_queue(context);

	return MOSQ_ERR_SUCCESS;
}


static int db__message_reconnect_reset_incoming(struct mosquitto *context)
{
	struct mosquitto__client_msg *client_msg, *tmp;

	context->msgs_in.inflight_bytes = 0;
	context->msgs_in.inflight_bytes12 = 0;
	context->msgs_in.inflight_count = 0;
	context->msgs_in.inflight_count12 = 0;
	context->msgs_in.queued_bytes = 0;
	context->msgs_in.queued_bytes12 = 0;
	context->msgs_in.queued_count = 0;
	context->msgs_in.queued_count12 = 0;
	context->msgs_in.inflight_quota = context->msgs_in.inflight_maximum;

	DL_FOREACH_SAFE(context->msgs_in.inflight, client_msg, tmp){
		db__msg_add_to_inflight_stats(&context->msgs_in, client_msg);
		if(client_msg->data.qos > 0){
			util__decrement_receive_quota(context);
		}

		if(client_msg->data.qos != 2){
			db__message_remove_inflight(context, &context->msgs_in, client_msg);
		}else{
			client_msg->data.dup = 0;
		}
	}

	DL_FOREACH_SAFE(context->msgs_in.queued, client_msg, tmp){
		client_msg->data.dup = 0;
		db__msg_add_to_queued_stats(&context->msgs_in, client_msg);
		if(db__ready_for_flight(context, mosq_md_in, client_msg->data.qos)){
			switch(client_msg->data.qos){
				case 0: client_msg->data.state = mosq_ms_publish_qos0; break;
				case 1: client_msg->data.state = mosq_ms_publish_qos1; break;
				case 2: client_msg->data.state = mosq_ms_publish_qos2; break;
			}
			db__message_dequeue_first(context, &context->msgs_in);
			plugin_persist__handle_client_msg_update(context, client_msg);
		}
	}

	return MOSQ_ERR_SUCCESS;
}


int db__message_reconnect_reset(struct mosquitto *context)
{
	int rc;

	rc = db__message_reconnect_reset_outgoing(context);
	if(rc){
		return rc;
	}
	return db__message_reconnect_reset_incoming(context);
}


int db__message_remove_incoming(struct mosquitto *context, uint16_t mid)
{
	struct mosquitto__client_msg *client_msg, *tmp;

	if(!context){
		return MOSQ_ERR_INVAL;
	}

	DL_FOREACH_SAFE(context->msgs_in.inflight, client_msg, tmp){
		if(client_msg->data.mid == mid){
			if(client_msg->base_msg->data.qos != 2){
				mosquitto_log_printf(MOSQ_LOG_INFO, "Protocol error from %s: Incorrect QoS (%d) when deleting incoming message.",
						context->id, client_msg->base_msg->data.qos);
				return MOSQ_ERR_PROTOCOL;
			}
			db__message_remove_inflight(context, &context->msgs_in, client_msg);
			return MOSQ_ERR_SUCCESS;
		}
	}

	return MOSQ_ERR_NOT_FOUND;
}


int db__message_release_incoming(struct mosquitto *context, uint16_t mid)
{
	struct mosquitto__client_msg *client_msg, *tmp;
	int retain;
	char *topic;
	char *source_id;
	bool deleted = false;
	int rc;

	if(!context){
		return MOSQ_ERR_INVAL;
	}

	DL_FOREACH_SAFE(context->msgs_in.inflight, client_msg, tmp){
		if(client_msg->data.mid == mid){
			if(client_msg->base_msg->data.qos != 2){
				mosquitto_log_printf(MOSQ_LOG_INFO, "Protocol error from %s: Incorrect QoS (%d) when releasing incoming message.",
						context->id, client_msg->base_msg->data.qos);
				return MOSQ_ERR_PROTOCOL;
			}
			topic = client_msg->base_msg->data.topic;
			retain = client_msg->data.retain;
			source_id = client_msg->base_msg->data.source_id;

			if(topic == NULL){
				db__message_remove_inflight(context, &context->msgs_in, client_msg);
				deleted = true;
			}else{
				rc = sub__messages_queue(source_id, topic, 2, retain, &client_msg->base_msg);
				if(rc == MOSQ_ERR_SUCCESS || rc == MOSQ_ERR_NO_SUBSCRIBERS){
					db__message_remove_inflight(context, &context->msgs_in, client_msg);
					deleted = true;
				}else{
					return 1;
				}
			}
		}
	}

	DL_FOREACH_SAFE(context->msgs_in.queued, client_msg, tmp){
		if(db__ready_for_flight(context, mosq_md_in, client_msg->data.qos)){
			break;
		}

		if(client_msg->data.qos == 2){
			send__pubrec(context, client_msg->data.mid, 0, NULL);
			client_msg->data.state = mosq_ms_wait_for_pubrel;
			db__message_dequeue_first(context, &context->msgs_in);
			plugin_persist__handle_client_msg_update(context, client_msg);
		}
	}
	if(deleted){
		return MOSQ_ERR_SUCCESS;
	}else{
		return MOSQ_ERR_NOT_FOUND;
	}
}


void db__expire_all_messages(struct mosquitto *context)
{
	struct mosquitto__client_msg *client_msg, *tmp;

	DL_FOREACH_SAFE(context->msgs_out.inflight, client_msg, tmp){
		if(client_msg->base_msg->data.expiry_time && db.now_real_s > client_msg->base_msg->data.expiry_time){
			if(client_msg->data.qos > 0){
				util__increment_send_quota(context);
			}
			db__message_remove_inflight(context, &context->msgs_out, client_msg);
		}
	}
	db__fill_inflight_out_from_queue(context);
	DL_FOREACH_SAFE(context->msgs_out.queued, client_msg, tmp){
		if(client_msg->base_msg->data.expiry_time && db.now_real_s > client_msg->base_msg->data.expiry_time){
			db__message_remove_queued(context, &context->msgs_out, client_msg);
		}
	}
	DL_FOREACH_SAFE(context->msgs_in.inflight, client_msg, tmp){
		if(client_msg->base_msg->data.expiry_time && db.now_real_s > client_msg->base_msg->data.expiry_time){
			if(client_msg->data.qos > 0){
				util__increment_receive_quota(context);
			}
			db__message_remove_inflight(context, &context->msgs_in, client_msg);
		}
	}
	DL_FOREACH_SAFE(context->msgs_in.queued, client_msg, tmp){
		if(client_msg->base_msg->data.expiry_time && db.now_real_s > client_msg->base_msg->data.expiry_time){
			db__message_remove_queued(context, &context->msgs_in, client_msg);
		}
	}
}


static void db__client_messages_check_acl(struct mosquitto *context, struct mosquitto_msg_data *msg_data,
		struct mosquitto__client_msg **head,
		void (*decrement_stats_fn)(struct mosquitto_msg_data *msg_data, struct mosquitto__client_msg *client_msg))
{
	struct mosquitto__client_msg *client_msg, *tmp;
	struct mosquitto__base_msg *base_msg;
	int access;

	DL_FOREACH_SAFE((*head), client_msg, tmp){
		base_msg = client_msg->base_msg;
		if(client_msg->data.direction == mosq_md_out){
			access = MOSQ_ACL_READ;
		}else{
			access = MOSQ_ACL_WRITE;
		}
		if(mosquitto_acl_check(context, base_msg->data.topic,
				base_msg->data.payloadlen, base_msg->data.payload,
				base_msg->data.qos, base_msg->data.retain,
				base_msg->data.properties, access) != MOSQ_ERR_SUCCESS){

			DL_DELETE((*head), client_msg);
			decrement_stats_fn(msg_data, client_msg);
			plugin_persist__handle_client_msg_delete(context, client_msg);
			db__msg_store_ref_dec(&client_msg->base_msg);
			mosquitto_FREE(client_msg);
		}
	}
}


void db__check_acl_of_all_messages(struct mosquitto *context)
{
	db__client_messages_check_acl(context, &context->msgs_in, &context->msgs_in.inflight, &db__msg_remove_from_inflight_stats);
	db__client_messages_check_acl(context, &context->msgs_in, &context->msgs_in.queued, &db__msg_remove_from_queued_stats);
	db__client_messages_check_acl(context, &context->msgs_out, &context->msgs_out.inflight, &db__msg_remove_from_inflight_stats);
	db__client_messages_check_acl(context, &context->msgs_out, &context->msgs_out.queued, &db__msg_remove_from_queued_stats);
}


static int db__message_write_inflight_out_single(struct mosquitto *context, struct mosquitto__client_msg *client_msg)
{
	struct mosquitto__base_msg *base_msg;
	mosquitto_property *base_msg_props = NULL;
	int rc;
	uint16_t mid;
	int retries;
	int retain;
	const char *topic;
	uint8_t qos;
	uint32_t payloadlen;
	const void *payload;
	uint32_t expiry_interval;
	uint32_t subscription_id;

	base_msg = client_msg->base_msg;

	expiry_interval = 0;
	if(base_msg->data.expiry_time){
		if(db.now_real_s > base_msg->data.expiry_time){
			if(client_msg->data.direction == mosq_md_out && client_msg->data.qos > 0){
				util__increment_send_quota(context);
			}
			db__message_remove_inflight(context, &context->msgs_out, client_msg);
			db__fill_inflight_out_from_queue(context);
			return MOSQ_ERR_SUCCESS;
		}else{
			expiry_interval = (uint32_t)(base_msg->data.expiry_time - db.now_real_s);
		}
	}
	mid = client_msg->data.mid;
	retries = client_msg->data.dup;
	retain = client_msg->data.retain;
	topic = base_msg->data.topic;
	qos = (uint8_t)client_msg->data.qos;
	payloadlen = base_msg->data.payloadlen;
	payload = base_msg->data.payload;
	subscription_id = client_msg->data.subscription_identifier;
	base_msg_props = base_msg->data.properties;

	switch(client_msg->data.state){
		case mosq_ms_publish_qos0:
			rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries, subscription_id, base_msg_props, expiry_interval);
			if(rc == MOSQ_ERR_SUCCESS || rc == MOSQ_ERR_OVERSIZE_PACKET){
				db__message_remove_inflight(context, &context->msgs_out, client_msg);
			}else{
				return rc;
			}
			break;

		case mosq_ms_publish_qos1:
			rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries, subscription_id, base_msg_props, expiry_interval);
			if(rc == MOSQ_ERR_SUCCESS){
				client_msg->data.dup = 1;
				client_msg->data.state = mosq_ms_wait_for_puback;
				plugin_persist__handle_client_msg_update(context, client_msg);
			}else if(rc == MOSQ_ERR_OVERSIZE_PACKET){
				db__message_remove_inflight(context, &context->msgs_out, client_msg);
			}else{
				return rc;
			}
			break;

		case mosq_ms_publish_qos2:
			rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries, subscription_id, base_msg_props, expiry_interval);
			if(rc == MOSQ_ERR_SUCCESS){
				client_msg->data.dup = 1;
				client_msg->data.state = mosq_ms_wait_for_pubrec;
				plugin_persist__handle_client_msg_update(context, client_msg);
			}else if(rc == MOSQ_ERR_OVERSIZE_PACKET){
				db__message_remove_inflight(context, &context->msgs_out, client_msg);
			}else{
				return rc;
			}
			break;

		case mosq_ms_resend_pubrel:
			rc = send__pubrel(context, mid, NULL);
			if(!rc){
				client_msg->data.state = mosq_ms_wait_for_pubcomp;
				plugin_persist__handle_client_msg_update(context, client_msg);
			}else{
				return rc;
			}
			break;

		case mosq_ms_invalid:
		case mosq_ms_send_pubrec:
		case mosq_ms_resend_pubcomp:
		case mosq_ms_wait_for_puback:
		case mosq_ms_wait_for_pubrec:
		case mosq_ms_wait_for_pubrel:
		case mosq_ms_wait_for_pubcomp:
		case mosq_ms_queued:
			break;
	}
	return MOSQ_ERR_SUCCESS;
}


int db__message_write_inflight_out_all(struct mosquitto *context)
{
	struct mosquitto__client_msg *client_msg, *tmp;
	int rc;

	if(context->state != mosq_cs_active || !net__is_connected(context)){
		return MOSQ_ERR_SUCCESS;
	}

	DL_FOREACH_SAFE(context->msgs_out.inflight, client_msg, tmp){
		rc = db__message_write_inflight_out_single(context, client_msg);
		if(rc){
			return rc;
		}
	}
	return MOSQ_ERR_SUCCESS;
}


int db__message_write_inflight_out_latest(struct mosquitto *context)
{
	struct mosquitto__client_msg *client_msg, *next;
	int rc;

	if(context->state != mosq_cs_active
			|| !net__is_connected(context)
			|| context->msgs_out.inflight == NULL){

		return MOSQ_ERR_SUCCESS;
	}

	if(context->msgs_out.inflight->prev == context->msgs_out.inflight){
		return db__message_write_inflight_out_single(context, context->msgs_out.inflight);
	}

	client_msg = context->msgs_out.inflight->prev;
	while(client_msg != context->msgs_out.inflight &&
			(client_msg->data.state == mosq_ms_publish_qos0
			|| client_msg->data.state == mosq_ms_publish_qos1
			|| client_msg->data.state == mosq_ms_publish_qos2)){

		client_msg = client_msg->prev;
	}

	if(client_msg != context->msgs_out.inflight){
		client_msg = client_msg->next;
	}

	while(client_msg){
		next = client_msg->next;
		rc = db__message_write_inflight_out_single(context, client_msg);
		if(rc){
			return rc;
		}
		client_msg = next;
	}
	return MOSQ_ERR_SUCCESS;
}


int db__message_write_queued_in(struct mosquitto *context)
{
	struct mosquitto__client_msg *client_msg, *tmp;
	int rc;

	if(context->state != mosq_cs_active){
		return MOSQ_ERR_SUCCESS;
	}

	DL_FOREACH_SAFE(context->msgs_in.queued, client_msg, tmp){
		if(context->msgs_in.inflight_maximum != 0 && context->msgs_in.inflight_quota == 0){
			break;
		}

		if(client_msg->data.qos == 2){
			client_msg->data.state = mosq_ms_send_pubrec;
			db__message_dequeue_first(context, &context->msgs_in);
			rc = send__pubrec(context, client_msg->data.mid, 0, NULL);
			if(!rc){
				client_msg->data.state = mosq_ms_wait_for_pubrel;
				plugin_persist__handle_client_msg_update(context, client_msg);
			}else{
				plugin_persist__handle_client_msg_update(context, client_msg);
				return rc;
			}
		}
	}
	return MOSQ_ERR_SUCCESS;
}


int db__message_write_queued_out(struct mosquitto *context)
{
	if(context->state != mosq_cs_active){
		return MOSQ_ERR_SUCCESS;
	}

	db__fill_inflight_out_from_queue(context);

	return MOSQ_ERR_SUCCESS;
}
