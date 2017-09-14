/*
 * Copyright (c) 2017 Alex Dubov <oakad@yahoo.com>
 *
 * This file is made available under the GNU General Public License
 * version 2 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
/*
 * May contain portions of MySQL Connector/J implementation
 *
 * Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * The MySQL Connector/J is licensed under the terms of the GPLv2
 * <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL
 * Connectors. There are special exceptions to the terms and conditions of
 * the GPLv2 as it is applied to this software, see the FOSS License Exception
 * <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
 */

package udentric.mysql.classic;

import udentric.mysql.util.BitsetEnum;

public enum ClientCapability implements BitsetEnum<Long> {
	LONG_PASSWORD(0, "new more secure passwords"),
	FOUND_ROWS(1, "found instead of affected rows"),
	LONG_FLAG(2, "get all column flags"),
	CONNECT_WITH_DB(3, "one can specify db on connect"),
	NO_SCHEMA(4, "don't allow database.table.column"),
	COMPRESS(5, "can use compression protocol"),
	ODBC(6, "odbc client"),
	LOCAL_FILES(7, "can use LOAD DATA LOCAL"),
	IGNORE_SPACE(8, "ignore spaces before '('"),
	PROTOCOL_41(9, "new 4.1 protocol"),
	INTERACTIVE(10, "this is an interactive client"),
	SSL(11, "switch to SSL after handshake"),
	IGNORE_SIGPIPE(12, "IGNORE sigpipes"),
	TRANSACTIONS(13, "client knows about transactions"),
	RESERVED(14, "old flag for 4.1 protocol"),
	SECURE_CONNECTION(15, "new 4.1 authentication"),
	MULTI_STATEMENTS(16, "enable/disable multi-stmt support"),
	MULTI_RESULTS(17, "enable/disable multi-results"),
	PS_MULTI_RESULTS(18, "multi-results in PS-protocol"),
	PLUGIN_AUTH(19, "client supports plugin authentication"),
	CONNECT_ATTRS(20, "client supports connection attributes"),
	PLUGIN_AUTH_LENENC_CLIENT_DATA(
		21,
		"enable authentication response packet to be larger than 255 bytes"
	),
	CAN_HANDLE_EXPIRED_PASSWORDS(
		22,
		"don't close the connection for a connection with expired password"
	),
	SESSION_TRACK(
		23, "capable of handling server state change information"
	),
	DEPRECATE_EOF(24, "client no longer needs EOF packet"),
	PROGRESS_OBSOLETE(29, ""),
	SSL_VERIFY_SERVER_CERT(30, "verify server SSL certificate"),
	REMEMBER_OPTIONS (31, "remember options"),
	MARIADB_PROGRESS(32, ""),
	MARIADB_COM_MULTI(33, ""),
	MARIADB_STMT_BULK_OPERATIONS(34, "support of array binding");

	private ClientCapability(int bitPos_, String desc_) {
		bitPos = bitPos_;
		desc = desc_;
	}

	@Override
	public boolean get(Long caps) {
		return ((caps >>> bitPos) & 1) == 1;
	}

	@Override
	public Long mask() {
		return 1L << bitPos;
	}

	public static String describe(Long caps) {
		StringBuilder sb = new StringBuilder();
		ClientCapability[] scs = ClientCapability.values();
		int cPos = 0;
		int ccPos = 0;

		while (caps != 0) {
			ClientCapability sc = null;
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

			caps >>>= 1;
			ccPos++;
		}
		return sb.toString();
	}

	private final int bitPos;
	private final String desc;
}
