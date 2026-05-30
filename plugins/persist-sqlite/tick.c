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

#include <string.h>
#include <sqlite3.h>
#include <stdlib.h>

#include "mosquitto/mqtt_protocol.h"
#include "mosquitto.h"
#include "mosquitto/broker.h"
#include "persist_sqlite.h"


int persist_sqlite__tick_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_tick *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	int wal_frames = 0;
	int wal_checkpointed = 0;

	UNUSED(event);

	if(ms->event_count > 0){
		ms->event_count = 0;

		/* Commit the current WAL transaction */
		sqlite3_exec(ms->db, "END;", NULL, NULL, NULL);

		/*
		 * RESTART checkpoint:
		 *   - Copies all WAL frames to the main DB file that are not
		 *     currently being read.
		 *   - Resets the WAL write position to the beginning so new
		 *     writes REUSE WAL space instead of appending.
		 *   - Does not require an exclusive lock (unlike TRUNCATE).
		 *   - Works even with concurrent readers (subscriber reloads).
		 *
		 * This keeps WAL bounded even under continuous read+write load.
		 * PASSIVE would skip busy frames and let WAL grow unboundedly.
		 */
		sqlite3_wal_checkpoint_v2(ms->db, NULL,
			SQLITE_CHECKPOINT_RESTART,
			&wal_frames, &wal_checkpointed);

		/*
		 * If all frames were checkpointed (no active readers held any
		 * back), truncate the WAL to zero bytes immediately.
		 */
		if(wal_frames > 0 && wal_frames == wal_checkpointed){
			sqlite3_wal_checkpoint_v2(ms->db, NULL,
				SQLITE_CHECKPOINT_TRUNCATE,
				NULL, NULL);
		}

		/* Begin next transaction */
		sqlite3_exec(ms->db, "BEGIN;", NULL, NULL, NULL);
	}

	/*
	 * Periodic VACUUM — reclaims space from deleted rows.
	 * vacuum_period is in ticks; each tick fires every flush_period seconds.
	 * Default: 720 ticks × 5s = 3600s = 1 hour.
	 * Set vacuum_period 0 in sqlite.conf to disable.
	 */
	ms->tick_count++;
	if(ms->vacuum_period > 0 && ms->tick_count >= ms->vacuum_period){
		ms->tick_count = 0;
		mosquitto_log_printf(MOSQ_LOG_INFO,
			"Sqlite persistence: starting VACUUM");

		/* VACUUM cannot run inside a transaction */
		sqlite3_exec(ms->db, "END;", NULL, NULL, NULL);

		/*
		 * Checkpoint with RESTART before VACUUM so the WAL is as
		 * empty as possible — VACUUM is faster on a clean WAL.
		 */
		sqlite3_wal_checkpoint_v2(ms->db, NULL,
			SQLITE_CHECKPOINT_RESTART, NULL, NULL);

		/*
		 * VACUUM rebuilds the DB file, reclaiming deleted row space.
		 * This may take seconds on a large DB — acceptable once/hour.
		 * After VACUUM, WAL is reset automatically by SQLite.
		 */
		int rc = sqlite3_exec(ms->db, "VACUUM;", NULL, NULL, NULL);
		if(rc != SQLITE_OK){
			mosquitto_log_printf(MOSQ_LOG_WARNING,
				"Sqlite persistence: VACUUM failed: %s",
				sqlite3_errmsg(ms->db));
		}else{
			mosquitto_log_printf(MOSQ_LOG_INFO,
				"Sqlite persistence: VACUUM complete");
		}

		/* Resume transaction after VACUUM */
		sqlite3_exec(ms->db, "BEGIN;", NULL, NULL, NULL);
	}

	ed->next_s = ms->flush_period;
	return MOSQ_ERR_SUCCESS;
}
