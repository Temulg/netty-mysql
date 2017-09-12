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

import com.google.common.collect.ImmutableMap;

public enum ColumnType {
	DECIMAL,
	TINY,
	SHORT,
	LONG,
	FLOAT,
	DOUBLE,
	NULL,
	TIMESTAMP,
	LONGLONG,
	INT24,
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

	public static ColumnType forId(int id) {
		return TYPES_BY_ID.get(id);
	}

	private static final ImmutableMap<
		Integer, ColumnType
	> TYPES_BY_ID;

	private final int id;
	static {
		ImmutableMap.Builder<
			Integer, ColumnType
		> builder = ImmutableMap.builder();

		for (ColumnType ct: ColumnType.values())
			builder.put(ct.id, ct);

		TYPES_BY_ID = builder.build();
	}
}
