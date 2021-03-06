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

package udentric.mysql;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.SucceededFuture;
import java.util.EnumMap;

public class MetadataQueries {
	private MetadataQueries() {
	}

	public static Future<PreparedStatement> tables(Channel ch) {
		return getMap(ch).get(QueryType.TABLES, ch);
	}

	public static Future<PreparedStatement> columns(Channel ch) {
		return getMap(ch).get(QueryType.COLUMNS, ch);
	}

	public static Future<PreparedStatement> primaryKeys(Channel ch) {
		return getMap(ch).get(QueryType.PRIMARY_KEYS, ch);
	}

	public static Future<PreparedStatement> importedKeys(Channel ch) {
		return getMap(ch).get(QueryType.IMPORTED_KEYS, ch);
	}

	public static Future<PreparedStatement> exportedKeys(Channel ch) {
		return getMap(ch).get(QueryType.EXPORTED_KEYS, ch);
	}

	public static Future<PreparedStatement> crossReference(Channel ch) {
		return getMap(ch).get(QueryType.CROSS_REFERENCE, ch);
	}

	public static Future<PreparedStatement> indexInfo(Channel ch) {
		return getMap(ch).get(QueryType.INDEX_INFO, ch);
	}

	public static Future<PreparedStatement> indexInfoUnique(Channel ch) {
		return getMap(ch).get(QueryType.INDEX_INFO_UNIQUE, ch);
	}

	public static Future<PreparedStatement> columnPrivileges(Channel ch) {
		return getMap(ch).get(QueryType.COLUMN_PRIVILEGES, ch);
	}

	public static Future<PreparedStatement> procedures(Channel ch) {
		return getMap(ch).get(QueryType.PROCEDURES, ch);
	}

	public static final String[] TABLE_TYPES = {
		"LOCAL TEMPORARY", "SYSTEM TABLE", "SYSTEM VIEW",
		"TABLE", "VIEW"
	};

	private static QueryMap getMap(Channel ch) {
		Attribute<QueryMap> attr = ch.attr(METADATA_QUERY_MAP);
		QueryMap qm = attr.get();

		if (qm == null)
			qm = attr.setIfAbsent(new QueryMap(ch));

		if (qm == null)
			qm = attr.get();

		return qm;
	}

	private static class QueryMap {
		QueryMap(Channel ch_) {
			ch = ch_;
		}

		synchronized Future<PreparedStatement> get(
			QueryType type, Channel ch
		) {
			Future<PreparedStatement> p = stmtMap.get(type);
			if (p != null)
				return p;

			Future<PreparedStatement> pp = Commands.with(
				ch
			).prepareStatement(type.query());

			stmtMap.put(type, pp);
			pp.addListener(fp -> {
				stmtPrepared(type, fp);
			});

			return pp;
		}

		synchronized void stmtPrepared(
			QueryType type, Future<?> fp
		) {
			Future<PreparedStatement> p = stmtMap.get(type);
			if (p != null) {
				if (p instanceof SucceededFuture)
					return;
			}

			if (!fp.isSuccess()) {
				stmtMap.remove(type);
				return;
			}

			PreparedStatement pp = (PreparedStatement)fp.getNow();
			stmtMap.put(
				type, ch.eventLoop().newSucceededFuture(pp)
			);
		}

		private final Channel ch;
		private final EnumMap<
			QueryType, Future<PreparedStatement>
		> stmtMap = new EnumMap<>(QueryType.class);
	}

	private static final AttributeKey<
		QueryMap
	> METADATA_QUERY_MAP = AttributeKey.valueOf(
		"udentric.mysql.MetadataQueries.QueryMap"
	);

	private static enum QueryType {
		TABLES {
			@Override
			String query() {
				return "SELECT TABLE_SCHEMA AS TABLE_CAT, "
				+ "TABLE_NAME, CASE WHEN "
				+ "TABLE_TYPE='BASE TABLE' THEN CASE WHEN "
				+ "TABLE_SCHEMA = 'mysql' "
				+ "OR TABLE_SCHEMA = 'performance_schema' "
				+ "THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN "
				+ "TABLE_TYPE='TEMPORARY' THEN 'LOCAL_TEMPORARY' "
				+ "ELSE TABLE_TYPE END AS TABLE_TYPE, "
				+ "TABLE_COMMENT AS REMARKS "
				+ "FROM INFORMATION_SCHEMA.TABLES WHERE "
				+ "TABLE_SCHEMA LIKE ? "
				+ "AND TABLE_NAME LIKE ? "
				+ "HAVING TABLE_TYPE IN (?, ?, ?, ?, ?) "
				+ "ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME";
			}
		},
		PRIMARY_KEYS {
			@Override
			String query() {
				return "SELECT TABLE_SCHEMA AS TABLE_CAT, "
				+ "TABLE_NAME, COLUMN_NAME, "
				+ "SEQ_IN_INDEX AS KEY_SEQ, "
				+ "'PRIMARY' AS PK_NAME "
				+ "FROM INFORMATION_SCHEMA.STATISTICS "
				+ "WHERE TABLE_SCHEMA LIKE ? "
				+ "AND TABLE_NAME LIKE ? "
				+ "AND INDEX_NAME='PRIMARY' "
				+ "ORDER BY TABLE_SCHEMA, TABLE_NAME, "
				+ "INDEX_NAME, SEQ_IN_INDEX";
			}
		},
		IMPORTED_KEYS {
			@Override
			String query() {
				return  KEY_QUERY_COMMON
				+ "USING (CONSTRAINT_NAME, TABLE_NAME) "
				+ "JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R "
				+ "ON (R.CONSTRAINT_NAME = B.CONSTRAINT_NAME "
				+ "AND R.TABLE_NAME = B.TABLE_NAME "
				+ "AND R.CONSTRAINT_SCHEMA = B.TABLE_SCHEMA) "
				+ "WHERE B.CONSTRAINT_TYPE = 'FOREIGN KEY' "
				+ "AND A.TABLE_SCHEMA LIKE ? AND A.TABLE_NAME = ? "
				+ "AND A.REFERENCED_TABLE_SCHEMA IS NOT NULL "
				+ "ORDER BY A.REFERENCED_TABLE_SCHEMA, "
				+ "A.REFERENCED_TABLE_NAME, A.ORDINAL_POSITION";
			}
		}, EXPORTED_KEYS {
			@Override
			String query() {
				return KEY_QUERY_COMMON
				+ "USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME) "
				+ "JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R "
				+ "ON (R.CONSTRAINT_NAME = B.CONSTRAINT_NAME "
				+ "AND R.TABLE_NAME = B.TABLE_NAME "
				+ "AND R.CONSTRAINT_SCHEMA = B.TABLE_SCHEMA) "
				+ "WHERE B.CONSTRAINT_TYPE = 'FOREIGN KEY' "
				+ "AND A.REFERENCED_TABLE_SCHEMA LIKE ? "
				+ "AND A.REFERENCED_TABLE_NAME = ? "
				+ "ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME, "
				+ "A.ORDINAL_POSITION";
			}			
		}, CROSS_REFERENCE {
			@Override
			String query() {
				return KEY_QUERY_COMMON
				+ "USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME) "
				+ "JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R "
				+ "ON (R.CONSTRAINT_NAME = B.CONSTRAINT_NAME "
				+ "AND R.TABLE_NAME = B.TABLE_NAME "
				+ "AND R.CONSTRAINT_SCHEMA = B.TABLE_SCHEMA) "
				+ "WHERE B.CONSTRAINT_TYPE = 'FOREIGN KEY' "
				+ "AND A.REFERENCED_TABLE_SCHEMA LIKE ? "
				+ "AND A.REFERENCED_TABLE_NAME = ? "
				+ "AND A.TABLE_SCHEMA LIKE ? "
				+ "AND A.TABLE_NAME = ? "
				+ "ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME, "
				+ "A.ORDINAL_POSITION";
			}
		}, INDEX_INFO {
			@Override
			String query() {
				return "SELECT TABLE_SCHEMA AS TABLE_CAT, "
				+ "TABLE_NAME, NON_UNIQUE, "
				+ "TABLE_SCHEMA AS INDEX_QUALIFIER, "
				+ "INDEX_NAME, SEQ_IN_INDEX AS ORDINAL_POSITION, "
				+ "COLUMN_NAME, COLLATION AS ASC_OR_DESC, "
				+ "CARDINALITY FROM INFORMATION_SCHEMA.STATISTICS "
				+ "WHERE TABLE_SCHEMA LIKE ? "
				+ "AND TABLE_NAME LIKE ? "
				+ "ORDER BY NON_UNIQUE, INDEX_NAME, "
				+ "SEQ_IN_INDEX";
			}
		}, INDEX_INFO_UNIQUE {
			@Override
			String query() {
				return "SELECT TABLE_SCHEMA AS TABLE_CAT, "
				+ "TABLE_NAME, NON_UNIQUE, "
				+ "TABLE_SCHEMA AS INDEX_QUALIFIER, "
				+ "INDEX_NAME, SEQ_IN_INDEX AS ORDINAL_POSITION, "
				+ "COLUMN_NAME, COLLATION AS ASC_OR_DESC, "
				+ "CARDINALITY FROM INFORMATION_SCHEMA.STATISTICS "
				+ "WHERE TABLE_SCHEMA LIKE ? "
				+ "AND TABLE_NAME LIKE ? AND NON_UNIQUE = 0 "
				+ "ORDER BY NON_UNIQUE, INDEX_NAME, "
				+ "SEQ_IN_INDEX";
			}
		}, COLUMNS {
			@Override
			String query() {
				return "SELECT TABLE_SCHEMA AS TABLE_CAT, "
				+ "TABLE_NAME, COLUMN_NAME, DATA_TYPE, "
				+ "CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 "
				+ "WHEN LCASE(DATA_TYPE)='time' THEN 8 "
				+ "WHEN LCASE(DATA_TYPE)='datetime' THEN 19 "
				+ "WHEN LCASE(DATA_TYPE)='timestamp' THEN 19 "
				+ "WHEN CHARACTER_MAXIMUM_LENGTH IS NULL "
				+ "THEN NUMERIC_PRECISION "
				+ "WHEN CHARACTER_MAXIMUM_LENGTH > "
				+ Integer.MAX_VALUE + " THEN " + Integer.MAX_VALUE
				+ " ELSE CHARACTER_MAXIMUM_LENGTH END AS COLUMN_SIZE, "
				+ "NUMERIC_SCALE AS DECIMAL_DIGITS, "
				+ "COLUMN_COMMENT AS REMARKS, "
				+ "COLUMN_DEFAULT AS COLUMN_DEF, "
				+ "CHARACTER_OCTET_LENGTH AS CHAR_OCTET_LENGTH, "
				+ "ORDINAL_POSITION, IS_NULLABLE, "
				+ "IF (EXTRA LIKE '%auto_increment%','YES','NO') AS IS_AUTOINCREMENT, "
				+ "IF (EXTRA LIKE '%GENERATED%','YES','NO') AS IS_GENERATEDCOLUMN "
				+ "FROM INFORMATION_SCHEMA.COLUMNS WHERE "
				+ "TABLE_SCHEMA LIKE ? "
				+ "AND TABLE_NAME LIKE ? "
				+ "ORDER BY TABLE_SCHEMA, TABLE_NAME, "
				+ "ORDINAL_POSITION";
			}
		}, COLUMN_PRIVILEGES {
			@Override
			String query() {
				return "SELECT TABLE_SCHEMA AS TABLE_CAT, "
				+ "TABLE_NAME, COLUMN_NAME, GRANTEE, "
				+ "PRIVILEGE_TYPE AS PRIVILEGE, "
				+ "IS_GRANTABLE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES "
				+ "WHERE TABLE_SCHEMA LIKE ? "
				+ "AND TABLE_NAME = ? AND COLUMN_NAME LIKE ? "
				+ "ORDER BY COLUMN_NAME, PRIVILEGE_TYPE";
			}
		}, PROCEDURES {
			@Override
			String query() {
				return "SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT, "
				+ "ROUTINE_NAME AS PROCEDURE_NAME, "
				+ "ROUTINE_COMMENT AS REMARKS, "
				+ "ROUTINE_TYPE AS PROCEDURE_TYPE, "
				+ "ROUTINE_NAME AS SPECIFIC_NAME "
				+ "FROM INFORMATION_SCHEMA.ROUTINES "
				+ "WHERE ROUTINE_SCHEMA LIKE ? "
				+ "AND ROUTINE_NAME LIKE ? "
				+ "ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME, "
				+ "ROUTINE_TYPE";
			}
		};

		abstract String query();

		private static final String KEY_QUERY_COMMON
		= "SELECT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT, "
		+ "A.REFERENCED_TABLE_NAME AS PKTABLE_NAME, "
		+ "A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, "
		+ "A.TABLE_SCHEMA AS FKTABLE_CAT, "
		+ "A.TABLE_NAME AS FKTABLE_NAME, "
		+ "A.COLUMN_NAME AS FKCOLUMN_NAME, "
		+ "A.ORDINAL_POSITION AS KEY_SEQ, "
		+ "R.UPDATE_RULE AS UPDATE_RULE, "
		+ "R.DELETE_RULE AS DELETE_RULE, "
		+ "A.CONSTRAINT_NAME AS FK_NAME, "
		+ "(SELECT CONSTRAINT_NAME FROM "
		+ "INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
		+ "WHERE TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA "
		+ "AND TABLE_NAME = A.REFERENCED_TABLE_NAME "
		+ "AND CONSTRAINT_TYPE IN ('UNIQUE', 'PRIMARY KEY') "
		+ "LIMIT 1) AS PK_NAME "
		+ "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A "
		+ "JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B ";
	}
}
