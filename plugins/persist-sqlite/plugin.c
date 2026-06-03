/*
Copyright (c) 2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

/* -------------------------------------------------------------------
 * Modifications:
 * - Added persistence-related events for SQLite plugin
 * - Extended MOSQ_EVT_* enums
 * - Modified structures for persistence integration
 *
 * Source:
 * https://github.com/mqttBridge/mosquitto.sqlite
 * ------------------------------------------------------------------- */

#include "config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifdef WIN32
#  include <direct.h>
#else
#  include <strings.h>
#endif

#include "mosquitto.h"
#include "mosquitto/broker.h"
#include "mosquitto/broker_plugin.h"
#include "mosquitto/mqtt_protocol.h"

#include "persist_sqlite.h"

MOSQUITTO_PLUGIN_DECLARE_VERSION(5);

static mosquitto_plugin_id_t *plg_id = NULL;
static struct mosquitto_sqlite plg_data;


static int conf_parse_uint(const char *in, const char *name,
                            unsigned int *value, int min_value)
{
	int v = atoi(in);
	if(v < min_value){
		mosquitto_log_printf(MOSQ_LOG_ERR,
			"Error: Invalid '%s' value %d in configuration.", name, v);
		return MOSQ_ERR_INVAL;
	}
	*value = (unsigned int)v;
	return MOSQ_ERR_SUCCESS;
}


static void set_defaults(void)
{
	/* "normal" synchronous mode */
	plg_data.synchronous   = 1;

	/* 5 seconds */
	plg_data.flush_period  = 5;

	plg_data.page_size     = 4 * 1024;

	/* 8 MB SQLite internal page cache */
	plg_data.cache_size_kb = 8192;

	plg_data.vacuum_period = 720;  /* 720 × 5s = 3600s = 1 hour */
}


static int get_db_file(struct mosquitto_opt *options, int option_count)
{
	const char *persistence_location;
	int i;

	persistence_location = mosquitto_persistence_location();
	if(persistence_location){
#ifdef WIN32
		(void)mkdir(persistence_location);
#else
		(void)mkdir(persistence_location, 0770);
#endif
		plg_data.db_file = malloc(
			strlen(persistence_location) + 1 +
			strlen("/mosquitto.sqlite3") + 1);
		if(!plg_data.db_file){
			mosquitto_log_printf(MOSQ_LOG_INFO,
				"Sqlite persistence: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
		sprintf(plg_data.db_file, "%s/mosquitto.sqlite3",
			persistence_location);
	}else{
		for(i=0; i<option_count; i++){
			if(!strcasecmp(options[i].key, "db_file")){
				plg_data.db_file = mosquitto_strdup(options[i].value);
				if(plg_data.db_file == NULL){
					return MOSQ_ERR_NOMEM;
				}
			}
		}
	}

	return MOSQ_ERR_SUCCESS;
}


/*
 * load_sqlite_conf — parse a separate SQLite config file.
 *
 * Format: one "key value" pair per line.
 * Lines starting with # or blank are ignored.
 *
 * Supported keys:
 *   db_file        — path to the SQLite database file
 *   sync           — off | normal | full | extra
 *   flush_period   — transaction commit interval in seconds (>= 0)
 *   page_size      — SQLite page size in bytes (>= 1)
 *   cache_size_kb  — SQLite internal page cache in KB (>= 0, 0 = SQLite default)
 *
 * Values already set by plugin_opt_ in mosquitto.conf take priority
 * (called after this function, so they overwrite).
 * db_file from persistence_location always wins over this file.
 */
static int load_sqlite_conf(const char *path, struct mosquitto_sqlite *ms)
{
	FILE *f = fopen(path, "r");
	if(!f){
		mosquitto_log_printf(MOSQ_LOG_ERR,
			"Sqlite persistence: cannot open config '%s': %s",
			path, strerror(errno));
		return MOSQ_ERR_INVAL;
	}

	char line[512];
	while(fgets(line, sizeof(line), f)){
		/* strip trailing newline / carriage return */
		line[strcspn(line, "\r\n")] = '\0';

		/* skip blank lines and comments */
		char *p = line;
		while(*p == ' ' || *p == '\t') p++;
		if(*p == '\0' || *p == '#') continue;

		/* split into key and value */
		char key[128], value[384];
		if(sscanf(p, "%127s %383[^\n]", key, value) != 2){
			mosquitto_log_printf(MOSQ_LOG_WARNING,
				"Sqlite persistence: ignoring malformed config line: %s",
				line);
			continue;
		}

		if(!strcasecmp(key, "db_file")){
			/* only use if persistence_location hasn't already set db_file */
			if(!ms->db_file){
				ms->db_file = mosquitto_strdup(value);
				if(!ms->db_file){
					fclose(f);
					return MOSQ_ERR_NOMEM;
				}
			}
		}else if(!strcasecmp(key, "sync")){
			if(!strcasecmp(value, "extra"))       ms->synchronous = 3;
			else if(!strcasecmp(value, "full"))   ms->synchronous = 2;
			else if(!strcasecmp(value, "normal")) ms->synchronous = 1;
			else if(!strcasecmp(value, "off"))    ms->synchronous = 0;
			else{
				mosquitto_log_printf(MOSQ_LOG_WARNING,
					"Sqlite persistence: unknown sync value '%s' in '%s'",
					value, path);
			}
		}else if(!strcasecmp(key, "flush_period")){
			int v = atoi(value);
			if(v >= 0) ms->flush_period = (unsigned int)v;
		}else if(!strcasecmp(key, "page_size")){
			int v = atoi(value);
			if(v >= 1) ms->page_size = (unsigned int)v;
		}else if(!strcasecmp(key, "cache_size_kb")){
			int v = atoi(value);
			if(v >= 0) ms->cache_size_kb = v;
		}else if(!strcasecmp(key, "vacuum_period")){
                  int v = atoi(value);
                  if(v >= 0) ms->vacuum_period = (unsigned int)v;
                }else{
			mosquitto_log_printf(MOSQ_LOG_WARNING,
				"Sqlite persistence: unknown config key '%s' in '%s'",
				key, path);
		}
	}

	fclose(f);
	mosquitto_log_printf(MOSQ_LOG_INFO,
		"Sqlite persistence: loaded config from '%s'", path);
	return MOSQ_ERR_SUCCESS;
}


int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier,
                           void **user_data,
                           struct mosquitto_opt *options,
                           int option_count)
{
	int i;
	int rc;

	UNUSED(user_data);

	memset(&plg_data, 0, sizeof(struct mosquitto_sqlite));
	set_defaults();

	/* ----------------------------------------------------------------
	 * Step 1: load separate sqlite.conf if plugin_opt_sqlite_conf set.
	 * This provides base values that plugin_opt_ keys can override.
	 * ---------------------------------------------------------------- */
	for(i=0; i<option_count; i++){
		if(!strcasecmp(options[i].key, "sqlite_conf")){
			plg_data.conf_file = mosquitto_strdup(options[i].value);
			if(!plg_data.conf_file) return MOSQ_ERR_NOMEM;
			rc = load_sqlite_conf(options[i].value, &plg_data);
			if(rc) return rc;
			break;
		}
	}

	/* ----------------------------------------------------------------
	 * Step 2: resolve db_file.
	 * persistence_location > plugin_opt_db_file > sqlite.conf db_file.
	 * ---------------------------------------------------------------- */
	if(get_db_file(options, option_count)){
		return MOSQ_ERR_UNKNOWN;
	}

	/* ----------------------------------------------------------------
	 * Step 3: plugin_opt_ keys override sqlite.conf values.
	 * ---------------------------------------------------------------- */
	for(i=0; i<option_count; i++){
		if(!strcasecmp(options[i].key, "sync")){
			if(!strcasecmp(options[i].value, "extra")){
				plg_data.synchronous = 3;
			}else if(!strcasecmp(options[i].value, "full")){
				plg_data.synchronous = 2;
			}else if(!strcasecmp(options[i].value, "normal")){
				plg_data.synchronous = 1;
			}else if(!strcasecmp(options[i].value, "off")){
				plg_data.synchronous = 0;
			}else{
				mosquitto_log_printf(MOSQ_LOG_ERR,
					"Sqlite persistence: Invalid plugin_opt_sync value '%s'.",
					options[i].value);
				return MOSQ_ERR_INVAL;
			}
		}else if(!strcasecmp(options[i].key, "flush_period")){
			rc = conf_parse_uint(options[i].value, "flush_period",
			                     &plg_data.flush_period, 0);
			if(rc) return rc;
		}else if(!strcasecmp(options[i].key, "page_size")){
			rc = conf_parse_uint(options[i].value, "page_size",
			                     &plg_data.page_size, 1);
			if(rc) return rc;
		}else if(!strcasecmp(options[i].key, "cache_size_kb")){
			rc = conf_parse_uint(options[i].value, "cache_size_kb",
			                     (unsigned int *)&plg_data.cache_size_kb, 0);
			if(rc) return rc;
		}
		/* sqlite_conf and db_file already handled above — skip silently */
	}

	/* ----------------------------------------------------------------
	 * Step 4: abort gracefully if no db_file resolved.
	 * ---------------------------------------------------------------- */
	if(plg_data.db_file == NULL){
		mosquitto_log_printf(MOSQ_LOG_WARNING,
			"Warning: Sqlite persistence plugin has no db_file defined. "
			"The plugin will not be activated.");
		return MOSQ_ERR_SUCCESS;
	}

	mosquitto_log_printf(MOSQ_LOG_INFO,
		"Sqlite persistence: db=%s sync=%d flush=%us page=%u cache=%dKB",
		plg_data.db_file, plg_data.synchronous, plg_data.flush_period,
		plg_data.page_size, plg_data.cache_size_kb);

	/* ----------------------------------------------------------------
	 * Step 5: open the database.
	 * ---------------------------------------------------------------- */
	rc = persist_sqlite__init(&plg_data);
	if(rc){
		return rc;
	}

	plg_id = identifier;

	/* ----------------------------------------------------------------
	 * Step 6: register all callbacks.
	 * ---------------------------------------------------------------- */
	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_RESTORE,
	        persist_sqlite__restore_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_BASE_MSG_ADD,
	        persist_sqlite__base_msg_add_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_BASE_MSG_DELETE,
	        persist_sqlite__base_msg_remove_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_BASE_MSG_LOAD,
	        persist_sqlite__base_msg_load_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_RETAIN_MSG_SET,
	        persist_sqlite__retain_msg_set_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_RETAIN_MSG_DELETE,
	        persist_sqlite__retain_msg_remove_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_CLIENT_ADD,
	        persist_sqlite__client_add_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_CLIENT_DELETE,
	        persist_sqlite__client_remove_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_CLIENT_UPDATE,
	        persist_sqlite__client_update_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_SUBSCRIPTION_ADD,
	        persist_sqlite__subscription_add_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_SUBSCRIPTION_DELETE,
	        persist_sqlite__subscription_remove_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_CLIENT_MSG_ADD,
	        persist_sqlite__client_msg_add_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_CLIENT_MSG_DELETE,
	        persist_sqlite__client_msg_remove_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_CLIENT_MSG_UPDATE,
	        persist_sqlite__client_msg_update_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_WILL_ADD,
	        persist_sqlite__will_add_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_PERSIST_WILL_DELETE,
	        persist_sqlite__will_remove_cb, NULL, &plg_data);
	if(rc) goto fail;

	rc = mosquitto_callback_register(plg_id, MOSQ_EVT_TICK,
	        persist_sqlite__tick_cb, NULL, &plg_data);
	if(rc) goto fail;

	return MOSQ_ERR_SUCCESS;

fail:
	if(rc == MOSQ_ERR_NOT_SUPPORTED){
		mosquitto_log_printf(MOSQ_LOG_ERR,
			"Sqlite persistence: Unable to register plugin: "
			"broker doesn't support persistence plugins, "
			"please upgrade to 2.1 or higher");
	}else if(rc == MOSQ_ERR_NOMEM){
		mosquitto_log_printf(MOSQ_LOG_ERR,
			"Sqlite persistence: Unable to register plugin: out of memory");
	}else{
		mosquitto_log_printf(MOSQ_LOG_ERR,
			"Sqlite persistence: Unable to register plugin (%d)", rc);
	}
	mosquitto_plugin_cleanup(NULL, NULL, 0);
	return rc;
}


int mosquitto_plugin_cleanup(void *user_data,
                              struct mosquitto_opt *options,
                              int option_count)
{
	UNUSED(user_data);
	UNUSED(options);
	UNUSED(option_count);

	if(plg_id){
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_RESTORE,
		        persist_sqlite__restore_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_BASE_MSG_ADD,
		        persist_sqlite__base_msg_add_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_BASE_MSG_DELETE,
		        persist_sqlite__base_msg_remove_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_BASE_MSG_LOAD,
		        persist_sqlite__base_msg_load_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_RETAIN_MSG_SET,
		        persist_sqlite__retain_msg_set_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_RETAIN_MSG_DELETE,
		        persist_sqlite__retain_msg_remove_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_CLIENT_ADD,
		        persist_sqlite__client_add_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_CLIENT_DELETE,
		        persist_sqlite__client_remove_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_CLIENT_UPDATE,
		        persist_sqlite__client_update_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_SUBSCRIPTION_ADD,
		        persist_sqlite__subscription_add_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_SUBSCRIPTION_DELETE,
		        persist_sqlite__subscription_remove_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_CLIENT_MSG_ADD,
		        persist_sqlite__client_msg_add_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_CLIENT_MSG_DELETE,
		        persist_sqlite__client_msg_remove_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_CLIENT_MSG_UPDATE,
		        persist_sqlite__client_msg_update_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_WILL_ADD,
		        persist_sqlite__will_add_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_PERSIST_WILL_DELETE,
		        persist_sqlite__will_remove_cb, NULL);
		mosquitto_callback_unregister(plg_id, MOSQ_EVT_TICK,
		        persist_sqlite__tick_cb, NULL);
	}

	mosquitto_free(plg_data.db_file);
	mosquitto_free(plg_data.conf_file);
	persist_sqlite__cleanup(&plg_data);
	memset(&plg_data, 0, sizeof(struct mosquitto_sqlite));

	return MOSQ_ERR_SUCCESS;
}
