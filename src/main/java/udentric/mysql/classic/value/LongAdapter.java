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

package udentric.mysql.classic.value;
import io.netty.buffer.ByteBuf;
import udentric.mysql.ErrorNumbers;
import udentric.mysql.MysqlString;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ColumnTypeTrait;
import udentric.mysql.classic.Field;
import udentric.mysql.classic.Packet;

public class LongAdapter implements JavaTypeAdapter {
	private LongAdapter() {
	}

	@Override
	public Object decodeTextValue(MysqlString value, Field fld) {
		if (ColumnTypeTrait.INTEGER.get(fld.type.traits)) {
			return Long.parseLong(value.toString());
		} else {
			Channels.throwAny(Packet.makeError(
				ErrorNumbers.ER_ILLEGAL_VALUE_FOR_TYPE
			));
			return null;
		}
	}

	public boolean encodeBinaryValue(
		ByteBuf dst, Object val, Field fld, int valueOffset,
		int softLimit
	) {
		throw new UnsupportedOperationException("Not implemented.");
	}

	public static LongAdapter INSTANCE = new LongAdapter();
}
