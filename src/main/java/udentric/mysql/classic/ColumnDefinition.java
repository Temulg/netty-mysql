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

import io.netty.buffer.ByteBuf;
import java.sql.SQLException;

import udentric.mysql.Encoding;
import udentric.mysql.MysqlErrorNumbers;

public class ColumnDefinition {
	public ColumnDefinition(int fieldCount) {
		fields = new Field[fieldCount];
	}

	public boolean hasAllFields() {
		return fieldPos == fields.length;
	}

	public void appendField(Field f) {
		fields[fieldPos] = f;
		fieldPos++;
	}

	public Row parseTextRow(ByteBuf src, Encoding enc) {
		TextRow row = new TextRow(fields.length);
		for (int pos = 0; pos < fields.length; ++pos)
			row.extractValue(
				pos, Packet.readIntLenenc(src), src, enc
			);

		return row;
	}

	public Field getField(int pos) throws SQLException {
		if (pos < 0 || pos >= fields.length) {
			throw Packet.makeErrorFromState(
				MysqlErrorNumbers.SQL_STATE_INVALID_COLUMN_NUMBER
			);
		}

		return fields[pos];
	}

	public Object getFieldValue(Row row, int pos, Class<?> cls) {
		return row.getFieldValue(pos, fields[pos], cls);
	}

	private final Field[] fields;
	private int fieldPos = 0;
}
