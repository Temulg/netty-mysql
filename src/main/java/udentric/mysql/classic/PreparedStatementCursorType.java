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

public enum PreparedStatementCursorType implements BitsetEnum<Integer> {
	NONE(0, ""),
	READ_ONLY(1, "read only cursor"),
	UPDATE(2, "update cursor"),
	SCROLLABLE(4, "");

	private PreparedStatementCursorType(int bitPos_, String desc_) {
		bitPos = bitPos_;
		desc = desc_;
	}

	@Override
	public boolean get(Integer bits) {
		return ((bits >>> bitPos) & 1) == 1;
	}

	@Override
	public Integer mask() {
		return 1 << bitPos;
	}

	public static String describe(Integer bits) {
		StringBuilder sb = new StringBuilder();
		PreparedStatementCursorType[] scs = PreparedStatementCursorType.values();
		int cPos = 0;
		int ccPos = 0;

		while (bits != 0) {
			PreparedStatementCursorType sc = null;
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

			if (1 == (bits & 1)) {
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

			bits = bits >>> 1;
			ccPos++;
		}
		return sb.toString();
	}

	private final int bitPos;
	private final String desc;
}
