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

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import java.sql.SQLException;

public class Field {
	public Field(
		ByteBuf src, CharsetInfo.Entry charset_
	) throws SQLException {
		Packet.skipBytesLenenc(src); // catalog

		schema = Packet.readStringLenenc(
			src, charset_.javaCharset
		);
		tableAlias = Packet.readStringLenenc(
			src, charset_.javaCharset
		);
		String s = Packet.readStringLenenc(
			src, charset_.javaCharset
		);
		tableName = s.equals(tableAlias) ? tableAlias : s;
		columnAlias = Packet.readStringLenenc(
			src, charset_.javaCharset
		);
		s = Packet.readStringLenenc(
			src, charset_.javaCharset
		);
		columnName = s.equals(columnAlias) ? columnAlias : s;

		src.skipBytes(1);

		charset = CharsetInfo.forId(Packet.readInt2(src));
		length = src.readIntLE();
		type = ColumnType.forId(Packet.readInt1(src));
		flags = src.readShortLE();
		decimalDigits = Packet.readInt1(src);
		src.skipBytes(2);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add(
			"schema", schema
		).add(
			"tableAlias", tableAlias
		).add(
			"tableName", tableName
		).add(
			"columnAlias", columnAlias
		).add(
			"columnName", columnName
		).add(
			"charset", charset.javaCharset
		).add(
			"length", length
		).add(
			"type", type
		).add(
			"flags", flags
		).add(
			"decimalDigits", decimalDigits
		).toString();
	}

	public final String schema;
	public final String tableAlias;
	public final String tableName;
	public final String columnAlias;
	public final String columnName;
	public final CharsetInfo.Entry charset;
	public final int length;
	public final ColumnType type;
	public final short flags;
	public final int decimalDigits;
}
