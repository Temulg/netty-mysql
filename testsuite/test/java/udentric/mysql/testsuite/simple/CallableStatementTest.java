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

import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import udentric.mysql.DataRow;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerAck;
import udentric.mysql.classic.ServerStatus;
import udentric.mysql.classic.SyncCommands;
import udentric.mysql.classic.dicta.ExecuteStatement;
import udentric.mysql.testsuite.TestCase;
import udentric.test.Assert;
import udentric.test.Tester;

public class CallableStatementTest extends TestCase {
	public CallableStatementTest() {
		super(Logger.getLogger(CallableStatementTest.class));
	}

	@Test
	public void noParams() throws Exception {
		createProcedure(
			"testSPNoParams", "()\nBEGIN\nSELECT 1;\nend\n"
		);

		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(),  "call testSPNoParams()"
		);

		Tester.beginAsync();

		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						(long)row.getValue(0), 1L
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
					if (!terminal)
						resultPos++;
					else {
						Assert.assertEquals(
							resultPos, 1
						);
						Assert.assertTrue(terminal);
						Assert.done();
					}
				}

				int resultPos = 0;
			}
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
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
							(int)row.getValue(0), 5
						);
						break;
					default:
						Assert.fail(
							"unexpected result set "
							+ resultPos
						);
					}
				}

				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					if (resultPos == 2)
						Assert.assertTrue(
							ServerStatus.PS_OUT_PARAMS.get(
								ack.srvStatus
							)
						);

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
			}, "abcd", "4"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void outParams() throws Exception {
		createProcedure(
			"testOutParam",
			"(x int, out y int)\nbegin\ndeclare z int;"
			+ "\nset z = x+1, y = z;\nend\n"
		);

		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(),  "call testOutParam(?, ?)"
		);

		Tester.beginAsync();

		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						(int)row.getValue(0), 6
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
					if (!terminal)
						resultPos++;
					else {
						Assert.assertEquals(
							resultPos, 1
						);
						Assert.assertTrue(terminal);
						Assert.done();
					}
				}

				int resultPos = 0;
			}, "5"
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}

	@Test
	public void resultSet() throws Exception {
		createTable("testSpResultTbl1", "(field1 INT)");
		
		SyncCommands.executeUpdate(
			channel(),
			"INSERT INTO testSpResultTbl1 VALUES (1), (2)"
		);
		
		createTable("testSpResultTbl2", "(field2 varchar(255))");

		SyncCommands.executeUpdate(
			channel(),
			"INSERT INTO testSpResultTbl2 VALUES ('abc'), ('def')"
		);

		createProcedure(
			"testSpResult",
			"()\nBEGIN\n"
			+ "SELECT field2 FROM testSpResultTbl2 WHERE field2='abc';\n"
			+ "UPDATE testSpResultTbl1 SET field1=2;\n"
			+ "SELECT field2 FROM testSpResultTbl2 WHERE field2='def';\n"
			+ "end\n"
		);

		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(),  "call testSpResult()"
		);

		Tester.beginAsync();

		channel().writeAndFlush(new ExecuteStatement(
			pstmt,
			new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					switch (resultPos) {
					case 0:
						Assert.assertEquals(
							row.getValue(0), "abc"
						);
						break;
					case 1:
						Assert.assertEquals(
							row.getValue(0), "def"
						);
						break;
					default:
						Assert.fail(
							"unexpected result set "
							+ resultPos
						);
					}
				}

				@Override
				public void acceptFailure(Throwable cause) {
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
							resultPos, 2
						);
						Assert.done();
					}
				}

				int resultPos = 0;
			}
		)).addListener(Channels::defaultSendListener);

		Tester.endAsync(1);
	}
}
