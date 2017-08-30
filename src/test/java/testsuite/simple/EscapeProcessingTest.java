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

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import testsuite.TestCase;

/**
 * Tests escape processing
 */
public class EscapeProcessingTest extends TestCase {
	public EscapeProcessingTest() {
		super(Logger.getLogger(EscapeProcessingTest.class));
	}

	@Test
	public void escapeProcessing() throws Exception {
		String results
			= "select dayname (abs(now())),   -- Today    \n"
			+ "           '1997-05-24',  -- a date                    \n"
			+ "           '10:30:29',  -- a time                     \n"
			+ (versionMeetsMinimum(5, 6, 4)
				? "           '1997-05-24 10:30:29.123', -- a timestamp  \n"
				: "           '1997-05-24 10:30:29', -- a timestamp  \n"
			) + "          '{string data with { or } will not be altered'   \n"
			+ "--  Also note that you can safely include { and } in comments";

		String exSql
			= "select {fn dayname ({fn abs({fn now()})})},   -- Today    \n"
			+ "           {d '1997-05-24'},  -- a date                    \n"
			+ "           {t '10:30:29' },  -- a time                     \n"
			+ "           {ts '1997-05-24 10:30:29.123'}, -- a timestamp  \n"
			+ "          '{string data with { or } will not be altered'   \n"
			+ "--  Also note that you can safely include { and } in comments";

		String escapedSql = conn().nativeSQL(exSql);
		Assert.assertEquals(escapedSql, results);
	}

	@Test
	public void convertEscape() throws Exception {
		Assert.assertEquals(
			conn().nativeSQL("{fn convert(abcd, SQL_INTEGER)}"),
			conn().nativeSQL("{fn convert(abcd, INTEGER)}")
		);
	}
/*
	public void timestampConversion() {
		TimeZone currentTimezone = TimeZone.getDefault();
		String[] availableIds = TimeZone.getAvailableIDs(
			currentTimezone.getRawOffset() + (3600 * 1000 * 2)
		);
		String newTimezone = null;

		if (availableIds.length > 0) {
			newTimezone = availableIds[0];
		} else {
			newTimezone = "UTC";
		}

		Properties props = new Properties();

		props.setProperty(
			PropertyDefinitions.PNAME_serverTimezone, newTimezone
		);
		Connection tzConn = null;

		try {
			String escapeToken = "SELECT {ts '2002-11-12 10:00:00'} {t '05:11:02'}";
			tzConn = getConnectionWithProps(props);
			Assert.assertNotEquals(
				tzConn.nativeSQL(escapeToken),
				this.conn.nativeSQL(escapeToken)
			);
		} finally {
			if (tzConn != null) {
				tzConn.close();
			}
		}
	}

	@Test
	public void bug51313() throws Exception {
		this.stmt = this.conn.createStatement();

		this.rs = this.stmt.executeQuery(
			"SELECT {fn lcase('My{fn UCASE(sql)}} -- DATABASE')}, {fn ucase({fn lcase('SERVER')})}"
			+ " -- {escape } processing test\n -- this {fn ucase('comment') is in line 2\r\n"
			+ " -- this in line 3, and previous escape sequence was malformed\n"
		);

		Assert.assertTrue(this.rs.next());
		Assert.assertEquals(
			this.rs.getString(1), "my{fn ucase(sql)}} -- database"
		);
		Assert.assertEquals(this.rs.getString(2), "SERVER");
		this.rs.close();

		this.rs = this.stmt.executeQuery(
			"SELECT 'MySQL \\\\\\' testing {long \\\\\\' escape -- { \\\\\\' sequences \\\\\\' } } with escape processing '"
		);
		Assert.assertTrue(this.rs.next());
		Assert.assertEquals(
			this.rs.getString(1),
			"MySQL \\\' testing {long \\\' escape -- { \\\' sequences \\\' } } with escape processing "
		);
		this.rs.close();

		this.rs = this.stmt.executeQuery(
			"SELECT 'MySQL \\'', '{ testing doubled -- } ''\\\\\\''' quotes '"
		);
		Assert.assertTrue(this.rs.next());
		Assert.assertEquals(this.rs.getString(1), "MySQL \'");
		Assert.assertEquals(
			this.rs.getString(2),
			"{ testing doubled -- } '\\\'' quotes "
		);
		this.rs.close();

		this.rs = this.stmt.executeQuery(
			"SELECT 'MySQL \\\\\\'''', '{ testing doubled -- } ''\\''' quotes '"
		);
		Assert.assertTrue(this.rs.next());
		Assert.assertEquals(this.rs.getString(1), "MySQL \\\''");
		Assert.assertEquals(
			this.rs.getString(2),
			"{ testing doubled -- } '\'' quotes "
		);
		this.rs.close();
	}
*/
}
