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

import java.sql.SQLException;

import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import udentric.mysql.DataRow;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerAck;
import udentric.mysql.classic.SyncCommands;
import udentric.mysql.classic.dicta.ExecuteStatement;
import udentric.mysql.classic.dicta.Query;
import udentric.mysql.testsuite.TestCase;
import udentric.test.Assert;
import udentric.test.Tester;

public class CallableStatementTest extends TestCase {
	public CallableStatementTest() {
		super(Logger.getLogger(CallableStatementTest.class));
	}

	@Test
	public void inOutParams() throws Exception {
		createProcedure(
			"testInOutParam",
			"(IN p1 VARCHAR(255), INOUT p2 INT)\n"
			+ "begin\n DECLARE z INT;\nSET z = p2 + 1;\n"
			+ "SET p2 = z;\n" + "SELECT p1;\n"
			+ "SELECT CONCAT('zyxw', p1);\nend\n"
		);

		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(),  "call testInOutParam(?, ?)"
		);

		SyncCommands.executeUpdate(channel(), "set @param0 = 4");

		Tester.beginAsync();

		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					switch (resultPos) {
					case 0:
						Assert.assertEquals(
							row.getValue(0), "abcd"
						);
						break;
					case 1:
						Assert.assertEquals(
							row.getValue(0),
							"zyxwabcd"
						);
						break;
					case 2:
						Assert.assertEquals(
							(int)row.getValue(0), 1
						);
						break;
					}
				}

				@Override
				public void acceptFailure(Throwable cause) {
					if (cause instanceof SQLException) {
						SQLException ex = (SQLException)cause;
						logger.error(String.format(
							"SQL error %s - %s",
							ex.getSQLState(),
							ex.getErrorCode()
						));
					}

					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					if (!terminal)
						resultPos++;
					else {
						Assert.assertEquals(
							resultPos, 3
						);
						Assert.done();
					}
				}

				int resultPos = 0;
			}, "abcd", "@param0"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);

		Tester.beginAsync();

		channel().writeAndFlush(new Query(
			"SELECT @param0",
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						(long)row.getValue(0), 4
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

/*
    public void testBatch() throws Exception {
        Connection batchedConn = null;

        try {
            createTable("testBatchTable", "(field1 INT)");
            createProcedure("testBatch", "(IN foo VARCHAR(15))\nbegin\nINSERT INTO testBatchTable VALUES (foo);\nend\n");

            executeBatchedStoredProc(this.conn);

            batchedConn = getConnectionWithProps("logger=StandardLogger,rewriteBatchedStatements=true,profileSQL=true");

            StandardLogger.startLoggingToBuffer();
            executeBatchedStoredProc(batchedConn);
            String[] log = StandardLogger.getBuffer().toString().split(";");
            assertTrue(log.length > 20);
        } finally {
            StandardLogger.dropBuffer();

            if (batchedConn != null) {
                batchedConn.close();
            }
        }
    }

    private void executeBatchedStoredProc(Connection c) throws Exception {
        this.stmt.executeUpdate("TRUNCATE TABLE testBatchTable");

        CallableStatement storedProc = c.prepareCall("{call testBatch(?)}");

        try {
            int numBatches = 300;

            for (int i = 0; i < numBatches; i++) {
                storedProc.setInt(1, i + 1);
                storedProc.addBatch();
            }

            int[] counts = storedProc.executeBatch();

            assertEquals(numBatches, counts.length);

            for (int i = 0; i < numBatches; i++) {
                assertEquals(1, counts[i]);
            }

            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBatchTable ORDER BY field1 ASC");

            for (int i = 0; i < numBatches; i++) {
                assertTrue(this.rs.next());
                assertEquals(i + 1, this.rs.getInt(1));
            }
        } finally {

            if (storedProc != null) {
                storedProc.close();
            }
        }
    }

    public void testOutParams() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testOutParam", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        storedProc = this.conn.prepareCall("{call testOutParam(?, ?)}");

        storedProc.setInt(1, 5);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        System.out.println(storedProc);

        int indexedOutParamToTest = storedProc.getInt(2);

        int namedOutParamToTest = storedProc.getInt("y");

        assertTrue("Named and indexed parameter are not the same", indexedOutParamToTest == namedOutParamToTest);
        assertTrue("Output value not returned correctly", indexedOutParamToTest == 6);

        // Start over, using named parameters, this time
        storedProc.clearParameters();
        storedProc.setInt("x", 32);
        storedProc.registerOutParameter("y", Types.INTEGER);

        storedProc.execute();

        indexedOutParamToTest = storedProc.getInt(2);
        namedOutParamToTest = storedProc.getInt("y");

        assertTrue("Named and indexed parameter are not the same", indexedOutParamToTest == namedOutParamToTest);
        assertTrue("Output value not returned correctly", indexedOutParamToTest == 33);

        try {
            storedProc.registerOutParameter("x", Types.INTEGER);
            assertTrue("Should not be able to register an out parameter on a non-out parameter", true);
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }

        try {
            storedProc.getInt("x");
            assertTrue("Should not be able to retreive an out parameter on a non-out parameter", true);
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_COLUMN_NOT_FOUND.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }

        try {
            storedProc.registerOutParameter(1, Types.INTEGER);
            assertTrue("Should not be able to register an out parameter on a non-out parameter", true);
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }
    }

    public void testResultSet() throws Exception {
        CallableStatement storedProc = null;

        createTable("testSpResultTbl1", "(field1 INT)");
        this.stmt.executeUpdate("INSERT INTO testSpResultTbl1 VALUES (1), (2)");
        createTable("testSpResultTbl2", "(field2 varchar(255))");
        this.stmt.executeUpdate("INSERT INTO testSpResultTbl2 VALUES ('abc'), ('def')");

        createProcedure("testSpResult", "()\nBEGIN\nSELECT field2 FROM testSpResultTbl2 WHERE field2='abc';\n"
                + "UPDATE testSpResultTbl1 SET field1=2;\nSELECT field2 FROM testSpResultTbl2 WHERE field2='def';\nend\n");

        storedProc = this.conn.prepareCall("{call testSpResult()}");

        storedProc.execute();

        this.rs = storedProc.getResultSet();

        ResultSetMetaData rsmd = this.rs.getMetaData();

        assertTrue(rsmd.getColumnCount() == 1);
        assertTrue("field2".equals(rsmd.getColumnName(1)));
        assertTrue(rsmd.getColumnType(1) == Types.VARCHAR);

        assertTrue(this.rs.next());

        assertTrue("abc".equals(this.rs.getString(1)));

        // TODO: This does not yet work in MySQL 5.0
        // assertTrue(!storedProc.getMoreResults());
        // assertTrue(storedProc.getUpdateCount() == 2);
        assertTrue(storedProc.getMoreResults());

        ResultSet nextResultSet = storedProc.getResultSet();

        rsmd = nextResultSet.getMetaData();

        assertTrue(rsmd.getColumnCount() == 1);
        assertTrue("field2".equals(rsmd.getColumnName(1)));
        assertTrue(rsmd.getColumnType(1) == Types.VARCHAR);

        assertTrue(nextResultSet.next());

        assertTrue("def".equals(nextResultSet.getString(1)));

        nextResultSet.close();

        this.rs.close();

        storedProc.execute();
    }

    public void testSPParse() throws Exception {

        CallableStatement storedProc = null;

        createProcedure("testSpParse", "(IN FOO VARCHAR(15))\nBEGIN\nSELECT 1;\nend\n");

        storedProc = this.conn.prepareCall("{call testSpParse()}");
        storedProc.close();
    }

    public void testSPNoParams() throws Exception {

        CallableStatement storedProc = null;

        createProcedure("testSPNoParams", "()\nBEGIN\nSELECT 1;\nend\n");

        storedProc = this.conn.prepareCall("{call testSPNoParams()}");
        storedProc.execute();
    }

    public void testSPCache() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testSpParse", "(IN FOO VARCHAR(15))\nBEGIN\nSELECT 1;\nend\n");

        int numIterations = 10;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numIterations; i++) {
            storedProc = this.conn.prepareCall("{call testSpParse(?)}");
            storedProc.close();
        }

        long elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Standard parsing/execution: " + elapsedTime + " ms");

        storedProc = this.conn.prepareCall("{call testSpParse(?)}");
        storedProc.setString(1, "abc");
        this.rs = storedProc.executeQuery();

        assertTrue(this.rs.next());
        assertTrue(this.rs.getInt(1) == 1);

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_cacheCallableStmts, "true");

        Connection cachedSpConn = getConnectionWithProps(props);

        startTime = System.currentTimeMillis();

        for (int i = 0; i < numIterations; i++) {
            storedProc = cachedSpConn.prepareCall("{call testSpParse(?)}");
            storedProc.close();
        }

        elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Cached parse stage: " + elapsedTime + " ms");

        storedProc = cachedSpConn.prepareCall("{call testSpParse(?)}");
        storedProc.setString(1, "abc");
        this.rs = storedProc.executeQuery();

        assertTrue(this.rs.next());
        assertTrue(this.rs.getInt(1) == 1);
    }

    public void testOutParamsNoBodies() throws Exception {
        CallableStatement storedProc = null;

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies, "true");

        Connection spConn = getConnectionWithProps(props);

        createProcedure("testOutParam", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        storedProc = spConn.prepareCall("{call testOutParam(?, ?)}");

        storedProc.setInt(1, 5);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        int indexedOutParamToTest = storedProc.getInt(2);

        assertTrue("Output value not returned correctly", indexedOutParamToTest == 6);

        storedProc.clearParameters();
        storedProc.setInt(1, 32);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        indexedOutParamToTest = storedProc.getInt(2);

        assertTrue("Output value not returned correctly", indexedOutParamToTest == 33);
    }

   public void testParameterParser() throws Exception {
        CallableStatement cstmt = null;

        try {

            createTable("t1", "(id   char(16) not null default '', data int not null)");
            createTable("t2", "(s   char(16),  i   int,  d   double)");

            createProcedure("foo42", "() insert into test.t1 values ('foo', 42);");
            this.conn.prepareCall("{CALL foo42()}");
            this.conn.prepareCall("{CALL foo42}");

            createProcedure("bar", "(x char(16), y int, z DECIMAL(10)) insert into test.t1 values (x, y);");
            cstmt = this.conn.prepareCall("{CALL bar(?, ?, ?)}");

            ParameterMetaData md = cstmt.getParameterMetaData();
            assertEquals(3, md.getParameterCount());
            assertEquals(Types.CHAR, md.getParameterType(1));
            assertEquals(Types.INTEGER, md.getParameterType(2));
            assertEquals(Types.DECIMAL, md.getParameterType(3));

            cstmt.close();

            createProcedure("p", "() label1: WHILE @a=0 DO SET @a=1; END WHILE");
            this.conn.prepareCall("{CALL p()}");

            createFunction("f", "() RETURNS INT NO SQL return 1; ");
            cstmt = this.conn.prepareCall("{? = CALL f()}");

            md = cstmt.getParameterMetaData();
            assertEquals(Types.INTEGER, md.getParameterType(1));
        } finally {
            if (cstmt != null) {
                cstmt.close();
            }
        }
    }
    */
}
