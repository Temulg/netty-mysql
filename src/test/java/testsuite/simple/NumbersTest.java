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

import java.sql.ResultSet;
import java.sql.SQLException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import testsuite.TestCase;

public class NumbersTest extends TestCase {
	public NumbersTest() {
		super(Logger.getLogger(NumbersTest.class));
	}

	@BeforeClass
	public void beforeClass() throws SQLException {
		System.err.format("--2- before class\n");
		createTable(
			"number_test",
			"(minBigInt bigint, maxBigInt bigint, testBigInt bigint)"
		);
		System.err.format("--2- create table\n");
		stmt().executeUpdate(String.format(
			"INSERT INTO number_test ("
			+ "minBigInt, maxBigInt, testBigInt"
			+ ") values (%d, %d, %d)",
			Long.MIN_VALUE, Long.MAX_VALUE, TEST_BIGINT_VALUE
		));
		System.err.format("--2- execute update\n");
	}

	@Test
	public void numbers() throws SQLException {
		System.err.format("--3- execute query\n");
		try (ResultSet rs = stmt().executeQuery(
			"SELECT * from number_test"
		)) {
			System.err.format("--3- result %s\n", rs);

			while (rs.next()) {
				long minBigInt = rs.getLong(1);
				long maxBigInt = rs.getLong(2);
				long testBigInt = rs.getLong(3);
				Assert.assertEquals(Long.MIN_VALUE, minBigInt);
				Assert.assertEquals(Long.MAX_VALUE, maxBigInt);
				Assert.assertEquals(
					testBigInt, TEST_BIGINT_VALUE
				);
			}
		}
	}

	private static final long TEST_BIGINT_VALUE = 6147483647L;
}
