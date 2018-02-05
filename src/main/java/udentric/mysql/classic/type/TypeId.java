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

package udentric.mysql.classic.type;

import com.google.common.collect.ImmutableMap;
import java.util.function.Supplier;

public enum TypeId {
	DECIMAL(0),
	TINY(1),
	SHORT(2),
	LONG(3),
	FLOAT(4),
	DOUBLE(5),
	NULL(6),
	TIMESTAMP(7),
	LONGLONG(8),
	INT24(9),
	DATE(10),
	TIME(11),
	DATETIME(12),
	YEAR(13),
	NEWDATE(14),
	VARCHAR(15),
	BIT(16),
	TIMESTAMP2(17),
	DATETIME2(18),
	TIME2(19),
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

	private TypeId(int id_) {
		id = id_;
		textAdapterSelector = (AdapterSelector)loadClass(
			String.format(
				"%s.text.T%04dSelector",
				TypeId.class.getPackage().getName(), id
			), () -> new AdapterSelector() {
				@Override
				public <T> ValueAdapter<T> get(Class<T> cls) {
					throw new UnsupportedOperationException(String.format(
						"Text adapter from MySQL type %s "
						+ "to object type %s not implemented",
						TypeId.this.name(),
						cls != null ? cls.getName() : "<unspecified>"
					));
				}

				@Override
				public <T> ValueAdapter<T> find(Class<T> cls) {
					throw new UnsupportedOperationException(String.format(
						"Text adapter from object class %s "
						+ "to MySQL type %s not implemented",
						cls != null ? cls.getName() : "<unspecified>",
						TypeId.this.name()
					));
				}
			}
		);

		binaryAdapterSelector = (AdapterSelector)loadClass(
			String.format(
				"%s.binary.T%04dSelector",
				TypeId.class.getPackage().getName(), id
			), () -> new AdapterSelector() {
				@Override
				public <T> ValueAdapter<T> get(Class<T> cls) {
					throw new UnsupportedOperationException(String.format(
						"Binary adapter from MySQL type %s "
						+ "to object type %s not implemented",
						TypeId.this.name(),
						cls != null ? cls.getName() : "<unspecified>"
					));
				}

				@Override
				public <T> ValueAdapter<T> find(Class<T> cls) {
					throw new UnsupportedOperationException(String.format(
						"Binary adapter from object class %s "
						+ "to MySQL type %s not implemented",
						cls != null ? cls.getName() : "<unspecified>",
						TypeId.this.name()
					));
				}
			}
		);
	}

	private static Object loadClass(String name, Supplier<Object> def) {
		try {
			Class<?> cls = TypeId.class.getClassLoader().loadClass(
				name
			);
			return cls.getDeclaredConstructor().newInstance();
		} catch (ReflectiveOperationException e) {
			return def.get();
		}
	}

	public static TypeId forId(int id) {
		return MYSQL_TYPE_BY_ID.get(id);
	}

	private static final ImmutableMap<
		Integer, TypeId
	> MYSQL_TYPE_BY_ID;

	public final int id;
	public final AdapterSelector textAdapterSelector;
	public final AdapterSelector binaryAdapterSelector;

	static {
		ImmutableMap.Builder<
			Integer, TypeId
		> builder = ImmutableMap.builder();

		for (TypeId t: TypeId.values())
			builder.put(t.id, t);

		MYSQL_TYPE_BY_ID = builder.build();
	}
}
