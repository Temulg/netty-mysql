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

package testsuite.simple;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import testsuite.TestCase;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ColumnDefinition;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.Row;
import udentric.mysql.classic.SyncCommands;
import udentric.mysql.classic.Packet.ServerAck;
import udentric.mysql.classic.dicta.Query;

public class NumbersTest extends TestCase {
	public NumbersTest() {
		super(Logger.getLogger(NumbersTest.class));
	}

	@BeforeClass
	public void beforeClass() throws SQLException {
		createTable(
			"number_test",
			"(minBigInt bigint, maxBigInt bigint, testBigInt bigint)"
		);

		SyncCommands.simpleQuery(channel(), String.format(
			"INSERT INTO number_test ("
			+ "minBigInt, maxBigInt, testBigInt"
			+ ") values (%d, %d, %d)",
			Long.MIN_VALUE, Long.MAX_VALUE, TEST_BIGINT_VALUE
		));
	}

	@Test
	public void numbers() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);

		channel().writeAndFlush(new Query(
			"SELECT * from number_test",
			new ResultSetConsumer(){
				@Override
				public void acceptRow(Row row) {
					Assert.assertEquals(
						colDef.getFieldValue(
							row, 0, Long.class
						),
						Long.MIN_VALUE
					);
					Assert.assertEquals(
						colDef.getFieldValue(
							row, 1, Long.class
						),
						Long.MAX_VALUE
					);
					Assert.assertEquals(
						colDef.getFieldValue(
							row, 2, Long.class
						),
						TEST_BIGINT_VALUE
					);
				}

				@Override
				public void acceptMetadata(
					ColumnDefinition colDef_
				) {
					colDef = colDef_;
				}
			
				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
					latch.countDown();
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					latch.countDown();
				}

				ColumnDefinition colDef;
			}
		)).addListener(Channels::defaultSendListener);
		latch.await();
	}

	private static final long TEST_BIGINT_VALUE = 6147483647L;
}
