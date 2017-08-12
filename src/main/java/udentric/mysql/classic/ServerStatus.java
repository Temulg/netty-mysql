/*
 * Copyright (c) 2017 Alex Dubov <oakad@yahoo.com>
 *
 * This file is made available under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package udentric.mysql.classic;

import udentric.mysql.util.BitsetEnum;

public enum ServerStatus implements BitsetEnum<Short> {
	IN_TRANS(0, ""),
	AUTOCOMMIT(1, "server in auto_commit mode"),
	MORE_RESULTS_EXISTS(3, "multi query - next query exists"),
	QUERY_NO_GOOD_INDEX_USED(4, ""),
	QUERY_NO_INDEX_USED(5, ""),
	CURSOR_EXISTS(6, ""),
	LAST_ROW_SENT(7, ""),
	DB_DROPPED(8, "a database was dropped"),
	NO_BACKSLASH_ESCAPES(9, ""),
	METADATA_CHANGED(10, ""),
	QUERY_WAS_SLOW(11, ""),
	PS_OUT_PARAMS(12, ""),
	IN_TRANS_READONLY(13, ""),
	SESSION_STATE_CHANGED (14, ""),
	ANSI_QUOTES(15, "");

	private ServerStatus(int bitPos_, String desc_) {
		bitPos = bitPos_;
		desc = desc_;
	}

	@Override
	public boolean get(Short caps) {
		return ((caps >>> bitPos) & 1) == 1;
	}

	@Override
	public Short mask() {
		return (short)(1 << bitPos);
	}

	public static String describe(Short caps) {
		StringBuilder sb = new StringBuilder();
		ServerStatus[] scs = ServerStatus.values();
		int cPos = 0;
		int ccPos = 0;

		while (caps != 0) {
			ServerStatus sc = null;
			if (cPos < scs.length) {
				sc = scs[cPos];
				while (sc.bitPos < ccPos) {
					cPos++;
					if (cPos < scs.length) {
						sc = scs[cPos];
					} else {
						sc = null;
						break;
					}
				}
			}

			if (1 == (caps & 1)) {
				if (sc != null && sc.bitPos == ccPos) {
					sb.append("bit ").append(ccPos).append(
						" set: "
					).append(sc.name()).append(
						" ("
					).append(sc.desc).append(")\n");
				} else {
					sb.append("bit ").append(
						ccPos
					).append(" set, but not defined\n");
				}
			}

			caps = (short)(caps >>> 1);
			ccPos++;
		}
		return sb.toString();
	}

	private final int bitPos;
	private final String desc;
}
