/*
 * Copyright (c) 2017 - 2018 Alex Dubov <oakad@yahoo.com>
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
 * May contain portions of MySQL Connector/J testsuite
 *
 * Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * The MySQL Connector/J is licensed under the terms of the GPLv2
 * <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL
 * Connectors. There are special exceptions to the terms and conditions of
 * the GPLv2 as it is applied to this software, see the FOSS License Exception
 * <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
 */

package udentric.mysql.testsuite.simple;

import java.math.BigDecimal;
import java.util.HashSet;

import com.google.common.collect.ObjectArrays;

import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import udentric.mysql.DataRow;
import udentric.mysql.FieldSet;
import udentric.mysql.MetadataQueries;
import udentric.mysql.PreparedStatement;
import udentric.mysql.ServerAck;
import udentric.mysql.SyncCommands;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ColumnValueMapper;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.SimpleColumnValueMapper;
import udentric.mysql.classic.dicta.ExecuteStatement;
import udentric.mysql.classic.dicta.Query;
import udentric.mysql.testsuite.TestCase;
import udentric.test.Assert;
import udentric.test.Tester;

public class MetadataTest extends TestCase {
	public MetadataTest() {
		super(Logger.getLogger(MetadataTest.class));
	}

	private void createTestTables() throws Exception {
		createTable(
			"parent",
			"(parent_id INT NOT NULL, PRIMARY KEY (parent_id))",
			"InnoDB"
		);
		createTable(
			"child",
			"(child_id INT, parent_id_fk INT, "
			+ "INDEX par_ind (parent_id_fk), "
			+ "FOREIGN KEY (parent_id_fk) "
			+ "REFERENCES parent(parent_id))",
			"InnoDB"
		);
		createTable(
			"cpd_foreign_1",
			"(id int(8) not null auto_increment primary key, "
			+ "name varchar(255) not null unique, key (id))",
			"InnoDB"
		);
		createTable(
			"cpd_foreign_2",
			"(id int(8) not null auto_increment primary key, "
			+ "key (id),name varchar(255))",
			"InnoDB"
		);
		createTable(
			"cpd_foreign_3",
			"(cpd_foreign_1_id int(8) not null, "
			+ "cpd_foreign_2_id int(8) not null, "
			+ "key(cpd_foreign_1_id),"
			+ "key(cpd_foreign_2_id), "
			+ "primary key (cpd_foreign_1_id, cpd_foreign_2_id), "
			+ "foreign key (cpd_foreign_1_id) references "
			+ "cpd_foreign_1(id), foreign key (cpd_foreign_2_id) "
			+ "references cpd_foreign_2(id))",
			"InnoDB"
		);
		createTable(
			"cpd_foreign_4",
			"(cpd_foreign_1_id int(8) not null, "
			+ "cpd_foreign_2_id int(8) not null, "
			+ "key(cpd_foreign_1_id),"
			+ "key(cpd_foreign_2_id), "
			+ "primary key (cpd_foreign_1_id, cpd_foreign_2_id), "
			+ "foreign key (cpd_foreign_1_id, cpd_foreign_2_id) "
			+ "references cpd_foreign_3("
			+ "cpd_foreign_1_id, cpd_foreign_2_id) "
			+ "ON DELETE RESTRICT ON UPDATE CASCADE)",
			"InnoDB"
		);
		createTable(
			"fktable1",
			"(TYPE_ID int not null, TYPE_DESC varchar(32), "
			+ "primary key(TYPE_ID))",
			"InnoDB"
		);
		createTable(
			"fktable2",
			"(KEY_ID int not null, COF_NAME varchar(32),"
			+ "PRICE float, TYPE_ID int, primary key(KEY_ID), "
			+ "index(TYPE_ID), foreign key(TYPE_ID) references "
			+ "fktable1(TYPE_ID))",
			"InnoDB"
		);
	}

	@Test
	public void foreignKeys() throws Exception {
		createTestTables();

		PreparedStatement pstmt = MetadataQueries.importedKeys(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					String pkColumnName = row.getValue(
						"PKCOLUMN_NAME"
					);
					String fkColumnName = row.getValue(
						"FKCOLUMN_NAME"
					);
					Assert.assertEquals(
						pkColumnName, "parent_id"
					);
					Assert.assertEquals(
						fkColumnName, "parent_id_fk"
					);
					resultPos++;
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				int resultPos = 0;
			},
			"testsuite", "child"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);

		pstmt = MetadataQueries.exportedKeys(channel()).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					String pkColumnName = row.getValue(
						"PKCOLUMN_NAME"
					);
					String fkColumnName = row.getValue(
						"FKCOLUMN_NAME"
					);
					String fkTableName = row.getValue(
						"FKTABLE_NAME"
					);
					Assert.assertEquals(
						pkColumnName, "parent_id"
					);
					Assert.assertEquals(
						fkTableName, "child"
					);
					Assert.assertEquals(
						fkColumnName, "parent_id_fk"
					);
					resultPos++;
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				int resultPos = 0;
			},
			"testsuite", "parent"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);

		pstmt = MetadataQueries.crossReference(channel()).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					String pkColumnName = row.getValue(
						"PKCOLUMN_NAME"
					);
					String pkTableName = row.getValue(
						"PKTABLE_NAME"
					);
					String fkColumnName = row.getValue(
						"FKCOLUMN_NAME"
					);
					String fkTableName = row.getValue(
						"FKTABLE_NAME"
					);
					String deleteAction = row.getValue(
						"DELETE_RULE"
					);
					String updateAction = row.getValue(
						"UPDATE_RULE"
					);

					Assert.assertEquals(
						pkTableName, "cpd_foreign_3"
					);
					Assert.assertEquals(
						fkTableName, "cpd_foreign_4"
					);
					Assert.assertEquals(
						deleteAction, "RESTRICT"
					);
					Assert.assertEquals(
						updateAction, "CASCADE"
					);
					switch (resultPos) {
					case 0:
						Assert.assertEquals(
							pkColumnName,
							"cpd_foreign_1_id"
						);
						Assert.assertEquals(
							fkColumnName,
							"cpd_foreign_1_id"
						);	
						break;
					case 1:
						Assert.assertEquals(
							pkColumnName,
							"cpd_foreign_2_id"
						);
						Assert.assertEquals(
							fkColumnName,
							"cpd_foreign_2_id"
						);	
						break;
					}
					resultPos++;
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 2);
					Assert.done();
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				int resultPos = 0;
			},
			"testsuite", "cpd_foreign_3", "testsuite",
			"cpd_foreign_4"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void getPrimaryKeys() throws Exception {
		createTable(
			"multikey",
			"(d INT NOT NULL, b INT NOT NULL, a INT NOT NULL, "
			+ "c INT NOT NULL, PRIMARY KEY (d, b, a, c))"
		);

		PreparedStatement pstmt = MetadataQueries.primaryKeys(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					String keyName = row.getValue(
						"COLUMN_NAME"
					);
					long keySeq = row.getValue("KEY_SEQ");
					switch (resultPos) {
					case 0:
						Assert.assertEquals(keySeq, 1);
						Assert.assertEquals(
							keyName, "d"
						);
						break;
					case 1:
						Assert.assertEquals(keySeq, 2);
						Assert.assertEquals(
							keyName, "b"
						);
						break;
					case 2:
						Assert.assertEquals(keySeq, 3);
						Assert.assertEquals(
							keyName, "a"
						);
						break;
					case 3:
						Assert.assertEquals(keySeq, 4);
						Assert.assertEquals(
							keyName, "c"
						);
						break;
					}
					resultPos++;
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 4);
					Assert.done();
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				int resultPos = 0;
			},
			"testsuite", "multikey"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void viewMetaData() throws Exception {
		createTable("testViewMetaData", "(field1 INT)");
		createView(
			"vTestViewMetaData",
			"AS SELECT field1 FROM testViewMetaData"
		);

		PreparedStatement pstmt = MetadataQueries.tables(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					String name = row.getValue(1);
					switch (resultPos) {
					case 0:
						Assert.assertEquals(
							name,
							"testViewMetaData"
						);
						break;
					case 1:
						Assert.assertEquals(
							name,
							"vTestViewMetaData"
						);
						break;
					}
					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(ServerAck ack, boolean terminal) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 2);
					Assert.done();
				}

				int resultPos = 0;
			},
			"testsuite", "%ViewMetaData", "TABLE", "VIEW"
		)).addListener(Channels::defaultSendListener);
		Tester.endAsync(1);

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					String name = row.getValue(1);
					Assert.assertEquals(
						name,
						"testViewMetaData"
					);

					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(ServerAck ack, boolean terminal) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos = 0;
			},
			"testsuite", "%ViewMetaData", "TABLE"
		)).addListener(Channels::defaultSendListener);
		Tester.endAsync(1);
	}

	@Test
	public void bitType() throws Exception {
		createTable(
			"testBitType", "(field1 BIT, field2 BIT, field3 BIT)"
		);
		SyncCommands.executeUpdate(
			channel(), 
			"INSERT INTO testBitType VALUES (1, 0, NULL)"
		);

		Tester.beginAsync();
		channel().writeAndFlush(new Query(
			"SELECT field1, field2, field3 FROM testBitType",
			new ResultSetConsumer() {
				@Override
				public ColumnValueMapper acceptMetadata(
					FieldSet columns
				) {
					return new SimpleColumnValueMapper(
						Boolean.class,
						Boolean.class,
						Boolean.class
					);
				}

				@Override
				public void acceptRow(DataRow row) {
					Assert.assertTrue(row.getValue(0));
					Assert.assertFalse(row.getValue(1));
					Assert.assertEquals(
						(Boolean)row.getValue(2), null
					);
				}
	
				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);	
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.done();
				}
			}
		)).addListener(Channels::defaultSendListener);
		Tester.endAsync(1);


		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(), 
			"SELECT field1, field2, field3 FROM testBitType"
		);

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public ColumnValueMapper acceptMetadata(
					FieldSet columns
				) {
					return new SimpleColumnValueMapper(
						Boolean.class,
						Boolean.class,
						Boolean.class
					);
				}

				@Override
				public void acceptRow(DataRow row) {
					Assert.assertTrue(row.getValue(0));
					Assert.assertFalse(row.getValue(1));
					Assert.assertEquals(
						(Boolean)row.getValue(2), null
					);
				}
	
				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);	
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.done();
				}
			}
		)).addListener(Channels::defaultSendListener);
		Tester.endAsync(1);
	}

	@Test
	public void tinyInt1IsBit() throws Exception {
		createTable("testTinyint1IsBit", "(field1 TINYINT(1))");
		SyncCommands.executeUpdate(
			channel(), "INSERT INTO testTinyint1IsBit VALUES (1)"
		);

		Tester.beginAsync();
		channel().writeAndFlush(new Query(
			"SELECT field1 FROM testTinyint1IsBit",
			new ResultSetConsumer(){
				@Override
				public ColumnValueMapper acceptMetadata(
					FieldSet columns
				) {
					return new SimpleColumnValueMapper(
						Boolean.class
					);
				}

				@Override
				public void acceptRow(DataRow row) {
					Assert.assertTrue(row.getValue(0));
				}
	
				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);	
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.done();
				}
			}
		)).addListener(Channels::defaultSendListener);
		Tester.endAsync(1);

		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(), "SELECT field1 FROM testTinyint1IsBit"
		);
		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer(){
				@Override
				public ColumnValueMapper acceptMetadata(
					FieldSet columns
				) {
					return new SimpleColumnValueMapper(
						Boolean.class
					);
				}

				@Override
				public void acceptRow(DataRow row) {
					Assert.assertTrue(row.getValue(0));
				}
	
				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.done();
				}
			}
		)).addListener(Channels::defaultSendListener);
		Tester.endAsync(1);
	}

	@Test
	public void getPrimaryKeysUsingInfoShcema() throws Exception {
		createTable("t1", "(c1 int(1) primary key)");

		PreparedStatement pstmt = MetadataQueries.primaryKeys(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("TABLE_NAME"),
						"t1"
					);
					Assert.assertEquals(
						row.getValue("COLUMN_NAME"),
						"c1"
					);
					resultPos++;
				}
			
				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "t1"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}


	@Test
	public void getIndexInfoUsingInfoSchema() throws Exception {
		createTable("t1", "(c1 int(1))");
		SyncCommands.executeUpdate(
			channel(), "CREATE INDEX index1 ON t1 (c1)"
		);

		PreparedStatement pstmt = MetadataQueries.indexInfo(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("TABLE_NAME"),
						"t1"
					);
					Assert.assertEquals(
						row.getValue("COLUMN_NAME"),
						"c1"
					);
					Assert.assertEquals(
						(long)row.getValue("NON_UNIQUE"),
						1
					);
					Assert.assertEquals(
						row.getValue("INDEX_NAME"),
						"index1"
					);
					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "t1"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void getColumnsUsingInfoSchema() throws Exception {
		createTable("t1", "(c1 char(1))");

		PreparedStatement pstmt = MetadataQueries.columns(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("TABLE_NAME"),
						"t1"
					);
					Assert.assertEquals(
						row.getValue("COLUMN_NAME"),
						"c1"
					);
					Assert.assertEquals(
						row.getValue("DATA_TYPE"),
						"char"
					);
					Assert.assertEquals(
						(BigDecimal)row.getValue("COLUMN_SIZE"),
						BigDecimal.ONE
					);
					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "t1"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void getTablesUsingInfoSchema() throws Exception {
		createTable("`t1-1`", "(c1 char(1))");
		createTable("`t1-2`", "(c1 char(1))");
		createTable("`t2`", "(c1 char(1))");
		HashSet<String> tableNames = new HashSet<>();
		tableNames.add("t1-1");
		tableNames.add("t1-2");

		PreparedStatement pstmt = MetadataQueries.tables(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					String name = row.getValue(
						"TABLE_NAME"
					);
					tableNames.remove(name);
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertTrue(tableNames.isEmpty());
					Assert.done();
				}
			}, ObjectArrays.concat(
				new String[] {"testsuite", "t1-_"},
				MetadataQueries.TABLE_TYPES, Object.class
			)
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void getColumnPrivilegesUsingInfoSchema() throws Exception {
		createTable("t1", "(c1 int)");
		String[] userAtHost = ((String)SyncCommands.selectColumn(
			channel(), "SELECT CURRENT_USER()", 0
		).get(0)).split("@");
		String userAtHostQuoted = String.format(
			"'%s'@'%s'", userAtHost[0], userAtHost[1]
		);

		SyncCommands.executeUpdate(
			channel(),
			"GRANT UPDATE (c1) ON t1 TO " + userAtHostQuoted
		);

		try {
			PreparedStatement pstmt = MetadataQueries.columnPrivileges(
				channel()
			).get();

			Tester.beginAsync();
			channel().writeAndFlush(new ExecuteStatement(
				pstmt, new ResultSetConsumer() {
					@Override
					public void acceptRow(DataRow row) {
						Assert.assertEquals(
							row.getValue(
								"TABLE_NAME"
							), "t1"
						);
						Assert.assertEquals(
							row.getValue(
								"COLUMN_NAME"
							), "c1"
						);
						Assert.assertEquals(
							row.getValue(
								"GRANTEE"
							), userAtHostQuoted
						);
						Assert.assertEquals(
							row.getValue(
								"PRIVILEGE"
							), "UPDATE"
						);
						resultPos++;
					}
    
					@Override
					public void acceptFailure(Throwable cause) {
						Assert.fail("query failed", cause);
					}

					@Override
					public void acceptAck(
						ServerAck ack, boolean terminal
					) {
						Assert.assertTrue(terminal);
						Assert.assertEquals(resultPos, 1);
						Assert.done();
					}

					int resultPos;
				}, "testsuite", "t1", "%"
			)).addListener(Channels::defaultSendListener);

			Tester.endAsync(1);
		} finally {
			SyncCommands.executeUpdate(
				channel(),
				"REVOKE UPDATE (c1) ON t1 FROM "
				+ userAtHostQuoted
			);
		}
	}

	@Test
	public void getProceduresUsingInfoSchema() throws Exception {
		createProcedure("sp1", "()\n BEGIN\nSELECT 1;end\n");

		PreparedStatement pstmt = MetadataQueries.procedures(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("PROCEDURE_NAME"),
						"sp1"
					);
					Assert.assertEquals(
						row.getValue("PROCEDURE_TYPE"),
						"PROCEDURE"
					);
					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "sp1"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void getCrossReferenceUsingInfoSchema() throws Exception {
		createTable(
			"parent", "(id INT NOT NULL, PRIMARY KEY (id))",
			"InnoDB"
		);
		createTable(
			"child",
			"(id INT, parent_id INT, FOREIGN KEY (parent_id) "
			+ "REFERENCES parent(id) ON DELETE SET NULL)",
			"InnoDB"
		);

		PreparedStatement pstmt = MetadataQueries.crossReference(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("PKTABLE_NAME"),
						"parent"
					);
					Assert.assertEquals(
						row.getValue("PKCOLUMN_NAME"),
						"id"
					);
					Assert.assertEquals(
						row.getValue("FKTABLE_NAME"),
						"child"
					);
					Assert.assertEquals(
						row.getValue("FKCOLUMN_NAME"),
						"parent_id"
					);

					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "parent", "testsuite", "child"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void getExportedKeysUsingInfoSchema() throws Exception {
		createTable(
			"parent",
			"(id INT NOT NULL, PRIMARY KEY (id))",
			"InnoDB"
		);
		createTable(
			"child",
			"(id INT, parent_id INT, FOREIGN KEY (parent_id) "
			+ "REFERENCES parent(id) ON DELETE SET NULL)",
			"InnoDB"
		);

		PreparedStatement pstmt = MetadataQueries.exportedKeys(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("PKTABLE_NAME"),
						"parent"
					);
					Assert.assertEquals(
						row.getValue("PKCOLUMN_NAME"),
						"id"
					);
					Assert.assertEquals(
						row.getValue("FKTABLE_NAME"),
						"child"
					);
					Assert.assertEquals(
						row.getValue("FKCOLUMN_NAME"),
						"parent_id"
					);

					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "parent"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void getImportedKeysUsingInfoSchema() throws Exception {
		createTable(
			"parent", "(id INT NOT NULL, PRIMARY KEY (id))",
			"InnoDB"
		);
		createTable(
			"child",
			"(id INT, parent_id INT, FOREIGN KEY (parent_id) "
			+ "REFERENCES parent(id) ON DELETE SET NULL)",
			"InnoDB"
		);

		PreparedStatement pstmt = MetadataQueries.importedKeys(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("PKTABLE_NAME"),
						"parent"
					);
					Assert.assertEquals(
						row.getValue("PKCOLUMN_NAME"),
						"id"
					);
					Assert.assertEquals(
						row.getValue("FKTABLE_NAME"),
						"child"
					);
					Assert.assertEquals(
						row.getValue("FKCOLUMN_NAME"),
						"parent_id"
					);

					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "child"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void generatedColumns() throws Exception {
		createTable(
			"pythagorean_triple",
			"(side_a DOUBLE NULL, side_b DOUBLE NULL, "
			+ "side_c_vir DOUBLE AS "
			+ "(SQRT(side_a * side_a + side_b * side_b)) "
			+ "VIRTUAL UNIQUE KEY COMMENT 'hypotenuse - virtual', "
			+ "side_c_sto DOUBLE GENERATED ALWAYS AS "
			+ "(SQRT(POW(side_a, 2) + POW(side_b, 2))) STORED "
			+ "UNIQUE KEY COMMENT 'hypotenuse - stored' "
			//+ "NOT NULL PRIMARY KEY"
			+ ")"
		);

		ServerAck ack = SyncCommands.executeUpdate(
			channel(),
			"INSERT INTO pythagorean_triple (side_a, side_b) "
			+ "VALUES (3, 4)"
		);
		Assert.assertEquals(ack.affectedRows(), 1);

		Tester.beginAsync();
		channel().writeAndFlush(new Query(
			"SELECT * FROM pythagorean_triple",
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue(0), 3.0
					);
					Assert.assertEquals(
						row.getValue(1), 4.0
					);
					Assert.assertEquals(
						row.getValue(2), 5.0
					);
					Assert.assertEquals(
						row.getValue(3), 5.0
					);
					Assert.assertEquals(
						row.getValue("side_a"), 3.0
					);
					Assert.assertEquals(
						row.getValue("side_b"), 4.0
					);
					Assert.assertEquals(
						row.getValue("side_c_sto"), 5.0
					);
					Assert.assertEquals(
						row.getValue("side_c_vir"), 5.0
					);

					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);

		PreparedStatement pstmt = MetadataQueries.columns(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					switch (resultPos) {
					case 0:
						Assert.assertEquals(
							row.getValue("COLUMN_NAME"),
							"side_a"
						);
						Assert.assertEquals(
							row.getValue("IS_NULLABLE"),
							"YES"
						);
						Assert.assertEquals(
							row.getValue("IS_AUTOINCREMENT"),
							"NO"
						);
						Assert.assertEquals(
							row.getValue("IS_GENERATEDCOLUMN"),
							"NO"
						);
						break;
					case 1:
						Assert.assertEquals(
							row.getValue("COLUMN_NAME"),
							"side_b"
						);
						Assert.assertEquals(
							row.getValue("IS_NULLABLE"),
							"YES"
						);
						Assert.assertEquals(
							row.getValue("IS_AUTOINCREMENT"),
							"NO"
						);
						Assert.assertEquals(
							row.getValue("IS_GENERATEDCOLUMN"),
							"NO"
						);
						break;
					case 2:
						Assert.assertEquals(
							row.getValue("COLUMN_NAME"),
							"side_c_vir"
						);
						Assert.assertEquals(
							row.getValue("IS_NULLABLE"),
							"YES"
						);
						Assert.assertEquals(
							row.getValue("IS_AUTOINCREMENT"),
							"NO"
						);
						Assert.assertEquals(
							row.getValue("IS_GENERATEDCOLUMN"),
							"YES"
						);
						break;
					case 3:
						Assert.assertEquals(
							row.getValue("COLUMN_NAME"),
							"side_c_sto"
						);
						Assert.assertEquals(
							row.getValue("IS_NULLABLE"),
							//"NO"
							"YES"
						);
						Assert.assertEquals(
							row.getValue("IS_AUTOINCREMENT"),
							"NO"
						);
						Assert.assertEquals(
							row.getValue("IS_GENERATEDCOLUMN"),
							"YES"
						);
						break;
					}
					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 4);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "pythagorean_triple"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
/*
		pstmt = MetadataQueries.primaryKeys(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue("COLUMN_NAME"),
						"side_c_sto"
					);
					Assert.assertEquals(
						row.getValue("PK_NAME"),
						"PRIMARY"
					);

					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 1);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "pythagorean_triple"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
*/

		pstmt = MetadataQueries.indexInfo(
			channel()
		).get();

		Tester.beginAsync();
		channel().writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					//Assert.assertEquals(row.getValue("INDEX_NAME"), "PRIMARY");
					//Assert.assertEquals(row.getValue("COLUMN_NAME"), "side_c_sto");

					switch (resultPos) {
					case 0:
						Assert.assertEquals(
							row.getValue("INDEX_NAME"),
							"side_c_sto"
						);
						Assert.assertEquals(
							row.getValue("COLUMN_NAME"),
							"side_c_sto"
						);
						break;
					case 1:
						Assert.assertEquals(
							row.getValue("INDEX_NAME"),
							"side_c_vir"
						);
						Assert.assertEquals(
							row.getValue("COLUMN_NAME"),
							"side_c_vir"
						);
						break;
					}

					resultPos++;
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.assertEquals(resultPos, 2);
					Assert.done();
				}

				int resultPos;
			}, "testsuite", "pythagorean_triple"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}
}
