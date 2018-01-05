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

import java.time.LocalDateTime;
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

public class DateTest extends TestCase {
	public DateTest() {
		super(Logger.getLogger(CallableStatementTest.class));
	}

	@Test
	public void timestamp() throws Exception {
		createTable(
			"DATETEST",
			"(tstamp TIMESTAMP, dt DATE, dtime DATETIME, tm TIME)"
		);

		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(), 
			"INSERT INTO DATETEST(tstamp, dt, dtime, tm) VALUES (?, ?, ?, ?)"
		);

		LocalDateTime ldt = LocalDateTime.of(
			2002, 6, 3, 7, 0, 0, 0
		);

		Tester.beginAsync();

		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer(){
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertEquals(
						ack.affectedRows, 1
					);
					Assert.done();
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}
			}, ldt, ldt.toLocalDate(), ldt, ldt.toLocalTime()
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);

		Tester.beginAsync();
		channel().writeAndFlush(new Query(
			"SELECT * from DATETEST",
			new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue(0), ldt
					);
					Assert.assertEquals(
						row.getValue(1),
						ldt.toLocalDate()
					);
					Assert.assertEquals(
						row.getValue(2), ldt
					);
					Assert.assertEquals(
						row.getValue(3),
						ldt.toLocalTime()
					);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.done();
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}				
			}
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);

		PreparedStatement psel = SyncCommands.prepareStatement(
			channel(), "SELECT * from DATETEST"
		);

		Tester.beginAsync();
		
		channel().writeAndFlush(new ExecuteStatement(
			psel,
			new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue(0), ldt
					);
					Assert.assertEquals(
						row.getValue(1),
						ldt.toLocalDate()
					);
					Assert.assertEquals(
						row.getValue(2), ldt
					);
					Assert.assertEquals(
						row.getValue(3),
						ldt.toLocalTime()
					);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					Assert.done();
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}				
			}
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}
/*
    public void testNanosParsing() throws SQLException {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testNanosParsing");
            this.stmt.executeUpdate("CREATE TABLE testNanosParsing (dateIndex int, field1 VARCHAR(32))");
            this.stmt.executeUpdate("INSERT INTO testNanosParsing VALUES (1, '1969-12-31 18:00:00.0'), (2, '1969-12-31 18:00:00.000000090'), "
                    + "(3, '1969-12-31 18:00:00.000000900'), (4, '1969-12-31 18:00:00.000009000'), (5, '1969-12-31 18:00:00.000090000'), "
                    + "(6, '1969-12-31 18:00:00.000900000'), (7, '1969-12-31 18:00:00.')");

            this.rs = this.stmt.executeQuery("SELECT field1 FROM testNanosParsing ORDER BY dateIndex ASC");
            assertTrue(this.rs.next());
            assertTrue(this.rs.getTimestamp(1).getNanos() == 0);
            assertTrue(this.rs.next());
            assertTrue(this.rs.getTimestamp(1).getNanos() + " != 90", this.rs.getTimestamp(1).getNanos() == 90);
            assertTrue(this.rs.next());
            assertTrue(this.rs.getTimestamp(1).getNanos() + " != 900", this.rs.getTimestamp(1).getNanos() == 900);
            assertTrue(this.rs.next());
            assertTrue(this.rs.getTimestamp(1).getNanos() + " != 9000", this.rs.getTimestamp(1).getNanos() == 9000);
            assertTrue(this.rs.next());
            assertTrue(this.rs.getTimestamp(1).getNanos() + " != 90000", this.rs.getTimestamp(1).getNanos() == 90000);
            assertTrue(this.rs.next());
            assertTrue(this.rs.getTimestamp(1).getNanos() + " != 900000", this.rs.getTimestamp(1).getNanos() == 900000);
            assertTrue(this.rs.next());

            try {
                this.rs.getTimestamp(1);
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testNanosParsing");
        }
    }

    public void testZeroDateBehavior() throws Exception {
        Connection testConn = this.conn;
        Connection roundConn = null;
        Connection nullConn = null;
        Connection exceptionConn = null;
        try {
            if (versionMeetsMinimum(5, 7, 4)) {
                Properties props = new Properties();
                props.setProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation, "false");
                if (versionMeetsMinimum(5, 7, 5)) {
                    String sqlMode = getMysqlVariable("sql_mode");
                    if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                        sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                        props.setProperty(PropertyDefinitions.PNAME_sessionVariables, "sql_mode='" + sqlMode + "'");
                    }
                }
                testConn = getConnectionWithProps(props);
                this.stmt = testConn.createStatement();
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testZeroDateBehavior");
            this.stmt.executeUpdate("CREATE TABLE testZeroDateBehavior(fieldAsString VARCHAR(32), fieldAsDateTime DATETIME)");
            this.stmt.executeUpdate("INSERT INTO testZeroDateBehavior VALUES ('0000-00-00 00:00:00', '0000-00-00 00:00:00')");

            roundConn = getConnectionWithProps("zeroDateTimeBehavior=ROUND");
            Statement roundStmt = roundConn.createStatement();
            this.rs = roundStmt.executeQuery("SELECT fieldAsString, fieldAsDateTime FROM testZeroDateBehavior");
            this.rs.next();

            assertEquals("0001-01-01", this.rs.getDate(1).toString());
            assertEquals("0001-01-01 00:00:00.0", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0", Locale.US).format(this.rs.getTimestamp(1)));
            assertEquals("0001-01-01", this.rs.getDate(2).toString());
            assertEquals("0001-01-01 00:00:00.0", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0", Locale.US).format(this.rs.getTimestamp(2)));

            PreparedStatement roundPrepStmt = roundConn.prepareStatement("SELECT fieldAsString, fieldAsDateTime FROM testZeroDateBehavior");
            this.rs = roundPrepStmt.executeQuery();
            this.rs.next();

            assertEquals("0001-01-01", this.rs.getDate(1).toString());
            assertEquals("0001-01-01 00:00:00.0", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0", Locale.US).format(this.rs.getTimestamp(1)));
            assertEquals("0001-01-01", this.rs.getDate(2).toString());
            assertEquals("0001-01-01 00:00:00.0", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0", Locale.US).format(this.rs.getTimestamp(2)));

            nullConn = getConnectionWithProps("zeroDateTimeBehavior=CONVERT_TO_NULL");
            Statement nullStmt = nullConn.createStatement();
            this.rs = nullStmt.executeQuery("SELECT fieldAsString, fieldAsDateTime FROM testZeroDateBehavior");

            this.rs.next();

            assertNull(this.rs.getDate(1));
            assertNull(this.rs.getTimestamp(1));
            assertNull(this.rs.getDate(2));
            assertNull(this.rs.getTimestamp(2));

            PreparedStatement nullPrepStmt = nullConn.prepareStatement("SELECT fieldAsString, fieldAsDateTime FROM testZeroDateBehavior");
            this.rs = nullPrepStmt.executeQuery();

            this.rs.next();

            assertNull(this.rs.getDate(1));
            assertNull(this.rs.getTimestamp(1));
            assertNull(this.rs.getDate(2));
            assertNull(this.rs.getTimestamp(2));

            exceptionConn = getConnectionWithProps("zeroDateTimeBehavior=EXCEPTION");
            Statement exceptionStmt = exceptionConn.createStatement();
            this.rs = exceptionStmt.executeQuery("SELECT fieldAsString, fieldAsDateTime FROM testZeroDateBehavior");

            this.rs.next();

            try {
                this.rs.getDate(1);
                fail("Exception should have been thrown when trying to retrieve invalid date");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }

            try {
                this.rs.getTimestamp(1);
                fail("Exception should have been thrown when trying to retrieve invalid date");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }

            try {
                this.rs.getDate(2);
                fail("Exception should have been thrown when trying to retrieve invalid date");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }

            try {
                this.rs.getTimestamp(2);
                fail("Exception should have been thrown when trying to retrieve invalid date");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }

            PreparedStatement exceptionPrepStmt = exceptionConn.prepareStatement("SELECT fieldAsString, fieldAsDateTime FROM testZeroDateBehavior");

            try {
                this.rs = exceptionPrepStmt.executeQuery();
                this.rs.next();
                this.rs.getDate(2);
                fail("Exception should have been thrown when trying to retrieve invalid date");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }

        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testZeroDateBehavior");
            if (exceptionConn != null) {
                exceptionConn.close();
            }

            if (nullConn != null) {
                nullConn.close();
            }

            if (roundConn != null) {
                roundConn.close();
            }

            if (testConn != this.conn) {
                testConn.close();
            }
        }
    }

    @SuppressWarnings("deprecation")
    public void testReggieBug() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testReggieBug");
            this.stmt.executeUpdate("CREATE TABLE testReggieBug (field1 DATE)");

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testReggieBug VALUES (?)");
            pStmt.setDate(1, new Date(2004 - 1900, 07, 28));
            pStmt.executeUpdate();
            this.rs = this.stmt.executeQuery("SELECT * FROM testReggieBug");
            this.rs.next();
            System.out.println(this.rs.getDate(1));
            this.rs = this.conn.prepareStatement("SELECT * FROM testReggieBug").executeQuery();
            this.rs.next();
            System.out.println(this.rs.getDate(1));

        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testReggieBug");
        }
    }

    public void testNativeConversions() throws Exception {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        Date dt = new Date(ts.getTime());
        Time tm = new Time(ts.getTime());

        createTable("testNativeConversions", "(time_field TIME, date_field DATE, datetime_field DATETIME, timestamp_field TIMESTAMP)");
        this.pstmt = this.conn.prepareStatement("INSERT INTO testNativeConversions VALUES (?,?,?,?)");
        this.pstmt.setTime(1, tm);
        this.pstmt.setDate(2, dt);
        this.pstmt.setTimestamp(3, ts);
        this.pstmt.setTimestamp(4, ts);
        this.pstmt.execute();
        this.pstmt.close();

        this.pstmt = this.conn.prepareStatement("SELECT time_field, date_field, datetime_field, timestamp_field FROM testNativeConversions");
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());
        System.out.println(this.rs.getTime(1));
        // DATE -> Time not allowed
        System.out.println(this.rs.getTime(3));
        System.out.println(this.rs.getTime(4));
        System.out.println();
        // TIME -> Date not allowed
        System.out.println(this.rs.getDate(2));
        System.out.println(this.rs.getDate(3));
        System.out.println(this.rs.getDate(4));
        System.out.println();
        System.out.println(this.rs.getTimestamp(1));
        System.out.println(this.rs.getTimestamp(2));
        System.out.println(this.rs.getTimestamp(3));
        System.out.println(this.rs.getTimestamp(4));
    }
    */
}
