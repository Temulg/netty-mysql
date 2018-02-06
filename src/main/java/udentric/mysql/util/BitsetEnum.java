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

package udentric.mysql.util;

import java.util.function.BiConsumer;

public interface BitsetEnum<BitsetType> {
	boolean get(BitsetType caps);

	BitsetType mask();

	int bitPos();

	String name();

	String description();

	static <T extends BitsetEnum<?>> void forEachSetBit(
		long bits, T[] values, BiConsumer<Integer, BitsetEnum<?>> cons
	) {
		int cPos = 0;
		int ccPos = 0;

		while (bits != 0) {
			BitsetEnum<?> val = null;
			if (cPos < values.length) {
				val = values[cPos];
				while (val.bitPos() < ccPos) {
					cPos++;
					if (cPos < values.length) {
						val = values[cPos];
					} else {
						val = null;
						break;
					}
				}
			}

			if (1 == (bits & 1)) {
				if (val != null && val.bitPos() == ccPos) {
					cons.accept(ccPos, val);
				} else {
					cons.accept(ccPos, null);
				}
			}

			bits = (short)(bits >>> 1);
			ccPos++;
		}
	}

	static <T extends BitsetEnum<?>> String describeLong(
		long bits, T[] values
	) {
		StringBuilder sb = new StringBuilder();
		forEachSetBit(bits, values, (pos, val) -> {
			if (val != null) {
				sb.append("bit ").append(pos).append(
					" set: "
				).append(val.name()).append(
					" ("
				).append(
					val.description()
				).append(")\n");
			} else {
				sb.append("bit ").append(
					pos
				).append(" set, but not defined\n");
			}
		});
		return sb.toString();
	}

	static <T extends BitsetEnum<?>> String describeShort(
		long bits, T[] values
	) {
		StringBuilder sb = new StringBuilder();
		forEachSetBit(bits, values, (pos, val) -> {
			if (sb.length() > 0)
				sb.append(", ");

			if (val != null) {
				sb.append(pos).append('(');
				sb.append(val.name()).append(')');
			} else {
				sb.append(pos).append('(');
				sb.append('*').append(')');
			}
		});
		return sb.toString();
	}
}
