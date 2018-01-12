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

import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import io.netty.util.concurrent.Future;
import udentric.mysql.DataRow;
import udentric.mysql.MetadataQueries;
import udentric.mysql.PreparedStatement;
import udentric.mysql.ServerAck;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.dicta.ExecuteStatement;
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

/*
    public void testRSMDIsReadOnly() throws Exception {
        try {
            this.rs = this.stmt.executeQuery("SELECT 1");

            ResultSetMetaData rsmd = this.rs.getMetaData();

            assertTrue(rsmd.isReadOnly(1));

            try {
                createTable("testRSMDIsReadOnly", "(field1 INT)");
                this.stmt.executeUpdate("INSERT INTO testRSMDIsReadOnly VALUES (1)");

                this.rs = this.stmt.executeQuery("SELECT 1, field1 + 1, field1 FROM testRSMDIsReadOnly");
                rsmd = this.rs.getMetaData();

                assertTrue(rsmd.isReadOnly(1));
                assertTrue(rsmd.isReadOnly(2));
                assertTrue(!rsmd.isReadOnly(3));
            } finally {
            }
        } finally {
            if (this.rs != null) {
                this.rs.close();
            }
        }
    }

    public void testBitType() throws Exception {
        try {
            createTable("testBitType", "(field1 BIT, field2 BIT, field3 BIT)");
            this.stmt.executeUpdate("INSERT INTO testBitType VALUES (1, 0, NULL)");
            this.rs = this.stmt.executeQuery("SELECT field1, field2, field3 FROM testBitType");
            this.rs.next();

            assertTrue(((Boolean) this.rs.getObject(1)).booleanValue());
            assertTrue(!((Boolean) this.rs.getObject(2)).booleanValue());
            assertEquals(this.rs.getObject(3), null);

            System.out.println(this.rs.getObject(1) + ", " + this.rs.getObject(2) + ", " + this.rs.getObject(3));

            this.rs = this.conn.prepareStatement("SELECT field1, field2, field3 FROM testBitType").executeQuery();
            this.rs.next();

            assertTrue(((Boolean) this.rs.getObject(1)).booleanValue());
            assertTrue(!((Boolean) this.rs.getObject(2)).booleanValue());

            assertEquals(this.rs.getObject(3), null);
            byte[] asBytesTrue = this.rs.getBytes(1);
            byte[] asBytesFalse = this.rs.getBytes(2);
            byte[] asBytesNull = this.rs.getBytes(3);

            assertEquals(asBytesTrue[0], 1);
            assertEquals(asBytesFalse[0], 0);
            assertEquals(asBytesNull, null);

            createTable("testBitField", "(field1 BIT(9))");
            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBitField");
            System.out.println(this.rs.getMetaData().getColumnClassName(1));
        } finally {
        }
    }

    public void testSupportsSelectForUpdate() throws Exception {
        boolean supportsForUpdate = this.conn.getMetaData().supportsSelectForUpdate();

        assertTrue(supportsForUpdate);
    }

    public void testTinyint1IsBit() throws Exception {
        String tableName = "testTinyint1IsBit";
        // Can't use 'BIT' or boolean
        createTable(tableName, "(field1 TINYINT(1))");
        this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1)");

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_tinyInt1isBit, "true");
        props.setProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean, "true");
        Connection boolConn = getConnectionWithProps(props);

        this.rs = boolConn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        checkBitOrBooleanType(false);

        this.rs = boolConn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
        checkBitOrBooleanType(false);

        this.rs = boolConn.getMetaData().getColumns(boolConn.getCatalog(), null, tableName, "field1");
        assertTrue(this.rs.next());

        assertEquals(Types.BOOLEAN, this.rs.getInt("DATA_TYPE"));

        assertEquals("BOOLEAN", this.rs.getString("TYPE_NAME"));

        props.clear();
        props.setProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean, "false");
        props.setProperty(PropertyDefinitions.PNAME_tinyInt1isBit, "true");

        Connection bitConn = getConnectionWithProps(props);

        this.rs = bitConn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        checkBitOrBooleanType(true);

        this.rs = bitConn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
        checkBitOrBooleanType(true);

        this.rs = bitConn.getMetaData().getColumns(boolConn.getCatalog(), null, tableName, "field1");
        assertTrue(this.rs.next());

        assertEquals(Types.BIT, this.rs.getInt("DATA_TYPE"));

        assertEquals("BIT", this.rs.getString("TYPE_NAME"));
    }

    private void checkBitOrBooleanType(boolean usingBit) throws SQLException {

        assertTrue(this.rs.next());
        assertEquals("java.lang.Boolean", this.rs.getObject(1).getClass().getName());
        if (!usingBit) {
            assertEquals(Types.BOOLEAN, this.rs.getMetaData().getColumnType(1));
        } else {
            assertEquals(Types.BIT, this.rs.getMetaData().getColumnType(1));
        }

        assertEquals("java.lang.Boolean", this.rs.getMetaData().getColumnClassName(1));
    }

    public void testGetPrimaryKeysUsingInfoShcema() throws Exception {
        createTable("t1", "(c1 int(1) primary key)");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getPrimaryKeys(null, null, "t1");
            this.rs.next();
            assertEquals("t1", this.rs.getString("TABLE_NAME"));
            assertEquals("c1", this.rs.getString("COLUMN_NAME"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    public void testGetIndexInfoUsingInfoSchema() throws Exception {
        createTable("t1", "(c1 int(1))");
        this.stmt.executeUpdate("CREATE INDEX index1 ON t1 (c1)");

        Connection conn1 = null;

        try {
            conn1 = getConnectionWithProps("useInformationSchema=true");
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getIndexInfo(conn1.getCatalog(), null, "t1", false, true);
            this.rs.next();
            assertEquals("t1", this.rs.getString("TABLE_NAME"));
            assertEquals("c1", this.rs.getString("COLUMN_NAME"));
            assertEquals("1", this.rs.getString("NON_UNIQUE"));
            assertEquals("index1", this.rs.getString("INDEX_NAME"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    public void testGetColumnsUsingInfoSchema() throws Exception {
        createTable("t1", "(c1 char(1))");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        props.setProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll, "true");
        props.setProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getColumns(null, null, "t1", null);
            this.rs.next();
            assertEquals("t1", this.rs.getString("TABLE_NAME"));
            assertEquals("c1", this.rs.getString("COLUMN_NAME"));
            assertEquals("CHAR", this.rs.getString("TYPE_NAME"));
            assertEquals("1", this.rs.getString("COLUMN_SIZE"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    public void testGetTablesUsingInfoSchema() throws Exception {
        createTable("`t1-1`", "(c1 char(1))");
        createTable("`t1-2`", "(c1 char(1))");
        createTable("`t2`", "(c1 char(1))");
        Set<String> tableNames = new HashSet<>();
        tableNames.add("t1-1");
        tableNames.add("t1-2");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            // pattern matching for table name
            this.rs = metaData.getTables(this.dbName, null, "t1-_", null);
            while (this.rs.next()) {
                assertTrue(tableNames.remove(this.rs.getString("TABLE_NAME")));
            }
            assertTrue(tableNames.isEmpty());
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
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
