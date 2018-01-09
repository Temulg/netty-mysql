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
import udentric.mysql.ServerAck;
import udentric.mysql.SyncCommands;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.dicta.ExecuteStatement;
import udentric.mysql.classic.dicta.Query;
import udentric.mysql.testsuite.TestCase;
import udentric.test.Assert;
import udentric.test.Tester;

public class DateTest extends TestCase {
	public DateTest() {
		super(Logger.getLogger(DateTest.class));
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
						ack.affectedRows(), 1
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
}
