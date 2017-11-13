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

import com.google.common.base.MoreObjects;

import io.netty.buffer.ByteBuf;
import udentric.mysql.Encoding;
import udentric.mysql.Field;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.TypeId;

import java.nio.charset.Charset;
import java.util.Objects;

public class FieldImpl implements Field {
	public FieldImpl(
		ByteBuf src, Charset cs
	) {
		Packet.skipBytesLenenc(src); // catalog

		schema = Packet.readStringLenenc(src, cs);
		tableAlias = Packet.readStringLenenc(src, cs);
		String s = Packet.readStringLenenc(src, cs);
		tableName = s.equals(tableAlias) ? tableAlias : s;
		columnAlias = Packet.readStringLenenc(src, cs);
		s = Packet.readStringLenenc(src, cs);
		columnName = s.equals(columnAlias) ? columnAlias : s;

		src.skipBytes(1);

		encoding = Encoding.forId(Packet.readInt2(src));
		length = src.readIntLE();
		type = TypeId.forId(Packet.readInt1(src));
		flags = src.readShortLE();
		decimalDigits = Packet.readInt1(src);
		src.skipBytes(2);
	}

	public int paramFlags() {
		return 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			schema, tableAlias, tableName, columnAlias, columnName,
			encoding.mysqlId, length, type, flags, decimalDigits
		);
	}

	@Override
	public boolean equals(Object other_) {
		if (!(other_ instanceof FieldImpl))
			return false;

		FieldImpl other = (FieldImpl)other_;

		return schema.equals(
			other.schema
		) && tableAlias.equals(
			other.tableAlias
		) && tableName.equals(
			other.tableName
		) && columnAlias.equals(
			other.columnAlias
		) && columnName.equals(
			other.columnName
		) && (
			encoding.mysqlId == other.encoding.mysqlId
		) && (
			length == other.length
		) && (
			type == other.type
		) && (
			flags == other.flags
		) && (
			decimalDigits == other.decimalDigits
		);
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
			"encoding", encoding
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

	public <T> T textValueDecode(
		ByteBuf src, int offset, int length, Class<T> cls
	) {
		return type.textAdapterSelector.get(cls).decodeValue(
			src, offset, length, this
		);
	}

	public void binaryValueEncode(
		ByteBuf dst, Object value, AdapterState state, int bufSoftLimit
	) {
		type.binaryAdapterSelector.find(value).encodeValue(
			dst, value, state, bufSoftLimit, this
		);
	}

	public <T> T binaryValueDecode(
		ByteBuf src, int offset, int length, Class<T> cls
	) {
		return type.binaryAdapterSelector.get(cls).decodeValue(
			src, offset, length, this
		);
	}

	public int binaryValueByteSize(ByteBuf src, int offset) {
		return type.binaryValueByteSize(src, offset, this);
	}

	public final String schema;
	public final String tableAlias;
	public final String tableName;
	public final String columnAlias;
	public final String columnName;
	public final Encoding encoding;
	public final int length;
	public final TypeId type;
	public final short flags;
	public final int decimalDigits;
}
