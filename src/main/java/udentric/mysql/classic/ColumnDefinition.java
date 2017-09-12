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
import udentric.mysql.exceptions.MysqlErrorNumbers;

public class ColumnDefinition {
	public ColumnDefinition(ByteBuf src, CharsetInfo.Entry charset_) {
		if (!"def".equals(Fields.readStringLenenc(
			src, charset_.javaCharset
		)))
			Packet.makeError(MysqlErrorNumbers.ER_MALFORMED_PACKET);
			
		schema = Fields.readStringLenenc(
			src, charset_.javaCharset
		);
		tableAlias = Fields.readStringLenenc(
			src, charset_.javaCharset
		);
		String s = Fields.readStringLenenc(
			src, charset_.javaCharset
		);
		table = s.equals(tableAlias) ? tableAlias : s;
		columnAlias = Fields.readStringLenenc(
			src, charset_.javaCharset
		);
		s = Fields.readStringLenenc(
			src, charset_.javaCharset
		);
		column = s.equals(columnAlias) ? columnAlias : s;

		fixedColumnSize = Fields.readLongLenenc(src);
		charset = CharsetInfo.forId(Fields.readInt2(src));
		maxColumnSize = src.readIntLE();
		type = ColumnType.forId(Fields.readInt1(src));
		flags = src.readShortLE();
		decimalDigits = Fields.readInt1(src);
		src.skipBytes(2);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add(
			"schema", schema
		).add(
			"tableAlias", tableAlias
		).add(
			"table", table
		).add(
			"columnAlias", columnAlias
		).add(
			"column", column
		).add(
			"fixedColumnSize", fixedColumnSize
		).add(
			"charset", charset
		).add(
			"maxColumnSize", maxColumnSize
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
	public final String table;
	public final String columnAlias;
	public final String column;
	public final long fixedColumnSize;
	public final CharsetInfo.Entry charset;
	public final int maxColumnSize;
	public final ColumnType type;
	public final short flags;
	public final int decimalDigits;
}
