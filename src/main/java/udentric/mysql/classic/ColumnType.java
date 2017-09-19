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

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import udentric.mysql.classic.value.DefaultAdapter;
import udentric.mysql.classic.value.JavaTypeAdapter;
import udentric.mysql.classic.value.LongAdapter;

public enum ColumnType {
	DECIMAL,
	TINY {
		@Override
		public Object readObject(ByteBuf src) {
			return src.readByte();
		}		
	},
	SHORT {
		@Override
		public Object readObject(ByteBuf src) {
			return src.readShortLE();
		}
	},
	LONG {
		@Override
		public Object readObject(ByteBuf src) {
			return src.readIntLE();
		}
	},
	FLOAT {
		@Override
		public Object readObject(ByteBuf src) {
			return Float.intBitsToFloat(src.readIntLE());
		}
	},
	DOUBLE {
		@Override
		public Object readObject(ByteBuf src) {
			return Double.longBitsToDouble(src.readLongLE());
		}
	},
	NULL,
	TIMESTAMP,
	LONGLONG {
		@Override
		public Object readObject(ByteBuf src) {
			return src.readLongLE();
		}
	},
	INT24 {
		@Override
		public Object readObject(ByteBuf src) {
			return src.readIntLE();
		}
	},
	DATE,
	TIME,
	DATETIME,
	YEAR,
	NEWDATE,
	VARCHAR,
	BIT,
	TIMESTAMP2,
	DATETIME2,
	TIME2,
	JSON(245),
	NEWDECIMAL(246),
	ENUM(247),
	SET(248),
	TINY_BLOB(249),
	MEDIUM_BLOB(250),
	LONG_BLOB(251),
	BLOB(252),
	VAR_STRING(253),
	STRING(254),
	GEOMETRY(255);

	private ColumnType() {
		id = ordinal();
	}

	private ColumnType(int id_) {
		id = id_;
	}

	public Object readObject(ByteBuf src) {
		throw new UnsupportedOperationException(
			"unsupported object type"
		);
	}

	public static ColumnType forId(int id) {
		return MYSQL_TYPE_BY_ID.get(id);
	}

	public static JavaTypeAdapter adapterForClass(Class<?> cls) {
		return ADAPTER_FOR_CLASS.getOrDefault(
			cls, DefaultAdapter.INSTANCE
		);
	}

	private static final ImmutableMap<
		Integer, ColumnType
	> MYSQL_TYPE_BY_ID;

	private static final ImmutableMap<
		Class<?>, JavaTypeAdapter
	> ADAPTER_FOR_CLASS = ImmutableMap.<
		Class<?>, JavaTypeAdapter
	>builder().put(
		Long.class, LongAdapter.INSTANCE
	).build();

	private final int id;
	static {
		ImmutableMap.Builder<
			Integer, ColumnType
		> builder = ImmutableMap.builder();

		for (ColumnType ct: ColumnType.values())
			builder.put(ct.id, ct);

		MYSQL_TYPE_BY_ID = builder.build();
	}
}
