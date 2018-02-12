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
/*
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

    public void testGetColumnPrivilegesUsingInfoSchema() throws Exception {

        if (!runTestIfSysPropDefined(PropertyDefinitions.SYSP_testsuite_cantGrant)) {
            Properties props = new Properties();

            props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
            props.setProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll, "true");
            props.setProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent, "true");
            Connection conn1 = null;
            Statement stmt1 = null;
            String userHostQuoted = null;

            boolean grantFailed = true;

            try {
                conn1 = getConnectionWithProps(props);
                stmt1 = conn1.createStatement();
                createTable("t1", "(c1 int)");
                this.rs = stmt1.executeQuery("SELECT CURRENT_USER()");
                this.rs.next();
                String user = this.rs.getString(1);
                List<String> userHost = StringUtils.split(user, "@", false);
                if (userHost.size() < 2) {
                    fail("This test requires a JDBC URL with a user, and won't work with the anonymous user. "
                            + "You can skip this test by setting the system property " + PropertyDefinitions.SYSP_testsuite_cantGrant);
                }
                userHostQuoted = "'" + userHost.get(0) + "'@'" + userHost.get(1) + "'";

                try {
                    stmt1.executeUpdate("GRANT update (c1) on t1 to " + userHostQuoted);

                    grantFailed = false;

                } catch (SQLException sqlEx) {
                    fail("This testcase needs to be run with a URL that allows the user to issue GRANTs "
                            + " in the current database. You can skip this test by setting the system property \""
                            + PropertyDefinitions.SYSP_testsuite_cantGrant + "\".");
                }

                if (!grantFailed) {
                    DatabaseMetaData metaData = conn1.getMetaData();
                    this.rs = metaData.getColumnPrivileges(null, null, "t1", null);
                    this.rs.next();
                    assertEquals("t1", this.rs.getString("TABLE_NAME"));
                    assertEquals("c1", this.rs.getString("COLUMN_NAME"));
                    assertEquals(userHostQuoted, this.rs.getString("GRANTEE"));
                    assertEquals("UPDATE", this.rs.getString("PRIVILEGE"));
                }
            } finally {
                if (stmt1 != null) {

                    if (!grantFailed) {
                        stmt1.executeUpdate("REVOKE UPDATE (c1) ON t1 FROM " + userHostQuoted);
                    }

                    stmt1.close();
                }

                if (conn1 != null) {
                    conn1.close();
                }
            }
        }
    }

    public void testGetProceduresUsingInfoSchema() throws Exception {
        createProcedure("sp1", "()\n BEGIN\nSELECT 1;end\n");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getProcedures(null, null, "sp1");
            this.rs.next();
            assertEquals("sp1", this.rs.getString("PROCEDURE_NAME"));
            assertEquals("1", this.rs.getString("PROCEDURE_TYPE"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    public void testGetCrossReferenceUsingInfoSchema() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        this.stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, " + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getCrossReference(null, null, "parent", null, null, "child");
            this.rs.next();
            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
            assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
            assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    public void testGetExportedKeysUsingInfoSchema() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        this.stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, " + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getExportedKeys(null, null, "parent");
            this.rs.next();
            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
            assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
            assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    public void testGetImportedKeysUsingInfoSchema() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        this.stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, " + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getImportedKeys(null, null, "child");
            this.rs.next();
            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
            assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
            assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    public void testGeneratedColumns() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        // Test GENERATED columns syntax.
        createTable("pythagorean_triple",
                "(side_a DOUBLE NULL, side_b DOUBLE NULL, "
                        + "side_c_vir DOUBLE AS (SQRT(side_a * side_a + side_b * side_b)) VIRTUAL UNIQUE KEY COMMENT 'hypotenuse - virtual', "
                        + "side_c_sto DOUBLE GENERATED ALWAYS AS (SQRT(POW(side_a, 2) + POW(side_b, 2))) STORED UNIQUE KEY COMMENT 'hypotenuse - stored' NOT NULL "
                        + "PRIMARY KEY)");

        // Test data for generated columns.
        assertEquals(1, this.stmt.executeUpdate("INSERT INTO pythagorean_triple (side_a, side_b) VALUES (3, 4)"));
        this.rs = this.stmt.executeQuery("SELECT * FROM pythagorean_triple");
        assertTrue(this.rs.next());
        assertEquals(3d, this.rs.getDouble(1));
        assertEquals(4d, this.rs.getDouble(2));
        assertEquals(5d, this.rs.getDouble(3));
        assertEquals(5d, this.rs.getDouble(4));
        assertEquals(3d, this.rs.getDouble("side_a"));
        assertEquals(4d, this.rs.getDouble("side_b"));
        assertEquals(5d, this.rs.getDouble("side_c_sto"));
        assertEquals(5d, this.rs.getDouble("side_c_vir"));
        assertFalse(this.rs.next());

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent, "true");

        for (String useIS : new String[] { "false", "true" }) {
            Connection testConn = null;
            props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, useIS);

            testConn = getConnectionWithProps(props);
            DatabaseMetaData dbmd = testConn.getMetaData();

            String test = "Case [" + props.toString() + "]";

            // Test columns metadata.
            this.rs = dbmd.getColumns(null, null, "pythagorean_triple", "%");
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_a", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "YES", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "NO", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_b", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "YES", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "NO", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_vir", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "YES", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "YES", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "NO", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "YES", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertFalse(test, this.rs.next());

            // Test primary keys metadata.
            this.rs = dbmd.getPrimaryKeys(null, null, "pythagorean_triple");
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "PRIMARY", this.rs.getString("PK_NAME"));
            assertFalse(test, this.rs.next());

            // Test indexes metadata.
            this.rs = dbmd.getIndexInfo(null, null, "pythagorean_triple", false, true);
            assertTrue(test, this.rs.next());
            assertEquals(test, "PRIMARY", this.rs.getString("INDEX_NAME"));
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_sto", this.rs.getString("INDEX_NAME"));
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_vir", this.rs.getString("INDEX_NAME"));
            assertEquals(test, "side_c_vir", this.rs.getString("COLUMN_NAME"));
            assertFalse(test, this.rs.next());

            testConn.close();
        }
    }
*/
}
