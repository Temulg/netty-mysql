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

import java.util.ArrayList;
import java.util.HashMap;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import io.netty.channel.Channel;
import udentric.mysql.Config;
import udentric.mysql.DataRow;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerAck;
import udentric.mysql.classic.SyncCommands;

import udentric.mysql.classic.dicta.Query;
import udentric.mysql.testsuite.TestCase;
import udentric.test.Assert;
import udentric.test.Tester;

public class CharsetTest extends TestCase {
	public CharsetTest() {
		super(Logger.getLogger(CallableStatementTest.class));
	}

	@Test
	public void cp932Backport() throws Exception {
		Channel ch = makeChannel(bld -> bld.withValue(
			Config.Key.characterEncoding, "cp932"
		));

		ch.close().await();
	}

	@Test
	public void necExtendedCharsByEUCJPSolaris() throws Exception {
		// 0x878A of WINDOWS-31J, NEC special(row13).
		char necExtendedChar = 0x3231;
		String necExtendedCharString = String.valueOf(necExtendedChar);

		createTable(
			"t_eucjpms",
			"(c1 char(1)) default character set = eucjpms"
		);

		Tester.beginAsync();

		Channel ch = makeChannel(bld -> bld.withValue(
			Config.Key.characterEncoding, "eucjpms"
		));

		SyncCommands.executeUpdate(
			ch,
			"INSERT INTO t_eucjpms VALUES ('"
			+ necExtendedCharString
			+ "')"
		);

		ch.writeAndFlush(new Query(
			"SELECT c1 FROM t_eucjpms",
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue(0),
						necExtendedCharString
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
		ch.close().await();

		Tester.beginAsync();
		ch = makeChannel();

		SyncCommands.executeUpdate(
			ch, "set character_set_results = eucjpms"
		);

		ch.writeAndFlush(new Query(
			"SELECT c1 FROM t_eucjpms",
			new ResultSetConsumer() {
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertEquals(
						row.getValue(0),
						necExtendedCharString
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
		ch.close().await();
	}

	@DataProvider(name = "charsets_0")
	public Object[][] charsetList0() {
		return new Object[][] {{
			"Shift_JIS", "sjis", SJIS_CHARS
		}, {
			"x-IBM943", "sjis", SJIS_CHARS
		}, {
			"windows-31j", "cp932", CP932_CHARS
		}, {
			"EUC-JP", "ujis", UJIS_CHARS
		}, {
			"x-eucJP-Open", "eucjpms", EUCJPMS_CHARS
		}};
	}

	@Test(dataProvider = "charsets_0")
	public void insertCharStatement(
		String javaCharset, String mysqlCharset, char[] testData
	) throws Exception {
		Channel ch_0 = makeChannel(bld -> bld.withValue(
			Config.Key.characterEncoding, mysqlCharset
		));

		Channel ch_1 = makeChannel();

		SyncCommands.executeUpdate(
			ch_1, "set character_set_results = " + mysqlCharset
		);

		SyncCommands.executeUpdate(
			ch_0, "DROP TABLE IF EXISTS t1"
		);
		SyncCommands.executeUpdate(
			ch_0,
			"CREATE TABLE t1 (c1 int, c2 char(1)) DEFAULT CHARACTER SET = "
			+ mysqlCharset
		);

		for (int pos = 0; pos < testData.length; pos++) {
			String refVal = String.valueOf(testData[pos]);
			SyncCommands.executeUpdate(ch_0, String.format(
				"INSERT INTO t1 values(%d, '%s')",
				pos, refVal
			));

			Tester.beginAsync();

			ch_0.writeAndFlush(new Query(
				"SELECT c2 FROM t1 WHERE c1 = " + pos,
				new ResultSetConsumer() {
					@Override
					public void acceptRow(DataRow row) {
						Assert.assertEquals(
							row.getValue(0),
							refVal
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

			ch_1.writeAndFlush(new Query(
				"SELECT c2 FROM t1 WHERE c1 = " + pos,
				new ResultSetConsumer() {
					@Override
					public void acceptRow(DataRow row) {
						Assert.assertEquals(
							row.getValue(0),
							refVal
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

			Tester.endAsync(2);
		}

		SyncCommands.executeUpdate(ch_0, "DROP TABLE t1");
		ch_1.close().await();
		ch_0.close().await();
	}
/*
	@Test
	public void gb18030() throws Exception {	
		// phrases to check
		String[][] str = new String[][] {{
			"C4EEC5ABBDBFA1A4B3E0B1DABBB3B9C520A1A4CBD5B6ABC6C2",
			"\u5FF5\u5974\u5A07\u00B7\u8D64\u58C1\u6000\u53E4 \u00B7\u82CF\u4E1C\u5761" 
		}, {
			"B4F3BDADB6ABC8A5A3ACC0CBCCD4BEA1A1A2C7A7B9C5B7E7C1F7C8CBCEEFA1A3",
			"\u5927\u6C5F\u4E1C\u53BB\uFF0C\u6D6A\u6DD8\u5C3D\u3001\u5343\u53E4\u98CE\u6D41\u4EBA\u7269\u3002"
		}, {
			"B9CAC0DDCEF7B1DFA3ACC8CBB5C0CAC7A1A2C8FDB9FAD6DCC0C9B3E0B1DAA1A3",
			"\u6545\u5792\u897F\u8FB9\uFF0C\u4EBA\u9053\u662F\u3001\u4E09\u56FD\u5468\u90CE\u8D64\u58C1\u3002"
		}, {
			"C2D2CAAFB1C0D4C6A3ACBEAACCCEC1D1B0B6A3ACBEEDC6F0C7A7B6D1D1A9A1A3",
			"\u4E71\u77F3\u5D29\u4E91\uFF0C\u60CA\u6D9B\u88C2\u5CB8\uFF0C\u5377\u8D77\u5343\u5806\u96EA\u3002"
		}, {
			"BDADC9BDC8E7BBADA3ACD2BBCAB1B6E0C9D9BAC0BDDCA3A1",
			"\u6C5F\u5C71\u5982\u753B\uFF0C\u4E00\u65F6\u591A\u5C11\u8C6A\u6770\uFF01"
		}, {
			"D2A3CFEBB9ABE8AAB5B1C4EAA3ACD0A1C7C7B3F5BCDEC1CBA3ACD0DBD7CBD3A2B7A2A1A3",
			"\u9065\u60F3\u516C\u747E\u5F53\u5E74\uFF0C\u5C0F\u4E54\u521D\u5AC1\u4E86\uFF0C\u96C4\u59FF\u82F1\u53D1\u3002"
		}, {
			"D3F0C9C8C2DABDEDA3ACCCB8D0A6BCE4A1A2E9C9E9D6BBD2B7C9D1CCC3F0A1A3",
			"\u7FBD\u6247\u7EB6\u5DFE\uFF0C\u8C08\u7B11\u95F4\u3001\u6A2F\u6A79\u7070\u98DE\u70DF\u706D\u3002"
		}, {
			"B9CAB9FAC9F1D3CEA3ACB6E0C7E9D3A6D0A6CED2A1A2D4E7C9FABBAAB7A2A1A3",
			"\u6545\u56FD\u795E\u6E38\uFF0C\u591A\u60C5\u5E94\u7B11\u6211\u3001\u65E9\u751F\u534E\u53D1\u3002"
		}, {
			"C8CBBCE4C8E7C3CEA3ACD2BBE9D7BBB9F5AABDADD4C2A1A3",
			"\u4EBA\u95F4\u5982\u68A6\uFF0C\u4E00\u6A3D\u8FD8\u9179\u6C5F\u6708\u3002"
		}, {
			"5373547483329330", "SsTt\uC23F"
		}, {
			"8239AB318239AB358239AF3583308132833087348335EB39",
			"\uB46C\uB470\uB498\uB7B5\uB7F3\uD47C"
		}, {
			"97339631973396339733A6359831C0359831C536",
			"\uD85A\uDC1F\uD85A\uDC21\uD85A\uDCC3\uD864\uDD27\uD864\uDD5A"
		}, {
			"9835CF329835CE359835F336",
			"\uD869\uDD6A\uD869\uDD63\uD869\uDED6"
		}, {
			"833988318339883283398539", "\uF45A\uF45B\uF444"
		}, {
			"823398318233973582339A3882348A32",
			"\u4460\u445A\u447B\u48C8"
		}, {
			"8134D5318134D6328134D832", "\u1817\u1822\u1836"
		}, {
			"4A7320204B82339A35646566", "Js  K\u4478def"
		}, {
			"8130883281308833", "\u00CE\u00CF"
		}, {
			"E05FE06A777682339230", "\u90F7\u9107wv\u4423"
		}, {
			"814081418139FE30", "\u4E02\u4E04\u3499"
		}, {
			"81308130FEFE", "\u0080\uE4C5"
		}, {
			"E3329A35E3329A34", "\uDBFF\uDFFF\uDBFF\uDFFE"
		}};

		HashMap<String, String> expected = new HashMap<String, String>();
	
		// check variables
		Connection con = getConnectionWithProps("characterEncoding=GB18030");
		Statement st = con.createStatement();
		ResultSet rset = st.executeQuery("show variables like 'character_set_client'");
		rset.next();
		assertEquals("gb18030", rset.getString(2));
		rset = st.executeQuery("show variables like 'character_set_connection'");
		rset.next();
		assertEquals("gb18030", rset.getString(2));
		rset = st.executeQuery("show variables like 'collation_connection'");
		rset.next();
		assertEquals("gb18030_chinese_ci", rset.getString(2));
	
		st.executeUpdate("DROP TABLE IF EXISTS testGB18030");
		st.executeUpdate("CREATE TABLE testGB18030(C VARCHAR(100) CHARACTER SET gb18030)");
	
		// insert phrases
		PreparedStatement pst = null;
		pst = con.prepareStatement("INSERT INTO testGB18030 VALUES(?)");
		for (int i = 0; i < str.length; i++) {
		    expected.put(str[i][0], str[i][1]);
		    pst.setString(1, str[i][1]);
		    pst.addBatch();
		}
		pst.executeBatch();
	
		// read phrases
		rset = st.executeQuery("SELECT c, HEX(c), CONVERT(c USING utf8mb4) FROM testGB18030");
		int resCount = 0;
		while (rset.next()) {
		    resCount++;
		    String hex = rset.getString(2);
		    assertTrue("HEX value " + hex + " for char " + rset.getString(1) + " is unexpected", expected.containsKey(hex));
		    assertEquals(expected.get(hex), rset.getString(1));
		    assertEquals(expected.get(hex), rset.getString(3));
		}
		assertEquals(str.length, resCount);
	
		// chars that can't be converted to utf8/utf16
		st.executeUpdate("TRUNCATE TABLE testGB18030");
		st.executeUpdate("INSERT INTO testGB18030 VALUES(0xFE39FE39FE38FE38),(0xFE39FE38A976)");
		rset = st.executeQuery("SELECT c, HEX(c), CONVERT(c USING utf8mb4) FROM testGB18030");
		while (rset.next()) {
		    String hex = rset.getString(2);
		    if ("FE39FE39FE38FE38".equals(hex)) {
			assertEquals("\uFFFD\uFFFD", rset.getString(1));
			assertEquals("??", rset.getString(3));
		    } else if ("FE39FE38A976".equals(hex)) {
			assertEquals("\uFFFD\uFE59", rset.getString(1));
			assertEquals("?\uFE59", rset.getString(3));
		    } else {
			fail("HEX value " + hex + " unexpected");
		    }
		}
	
		st.executeUpdate("DROP TABLE IF EXISTS testGB18030");
		con.close();
	
	    }
	}
*/
	/**
	 * Test data of sjis. sjis consists of ASCII, JIS-Roman, JISX0201 and
	 * JISX0208.
	 */
	public static final char[] SJIS_CHARS = new char[] {
		0xFF71, // halfwidth katakana letter A, 0xB100 of SJIS, one of JISX0201.
		0x65E5, // CJK unified ideograph, 0x93FA of SJIS, one of JISX0208.
		0x8868, // CJK unified ideograph, 0x955C of SJIS, one of '5c' character.
		0x2016 // 0x8161 of SJIS/WINDOWS-31J, converted to differently to/from ucs2
	};

	/**
	 * Test data of cp932. WINDOWS-31J consists of ASCII, JIS-Roman, JISX0201,
	 * JISX0208, NEC special characters(row13), NEC selected IBM special
	 * characters, and IBM special characters.
	 */
	private static final char[] CP932_CHARS = new char[] {
		0xFF71, // halfwidth katakana letter A, 0xB100 of WINDOWS-31J, one of JISX0201.
		0x65E5, // CJK unified ideograph, 0x93FA of WINDOWS-31J, one of JISX0208.
		0x3231, // parenthesized ideograph stok, 0x878B of WINDOWS-31J, one of NEC special characters(row13).
		0x67BB, // CJK unified ideograph, 0xEDC6 of WINDOWS-31J, one of NEC selected IBM special characters.
		0x6D6F, // CJK unified ideograph, 0xFAFC of WINDOWS-31J, one of IBM special characters.
		0x8868, // one of CJK unified ideograph, 0x955C of WINDOWS-31J, one of '5c' characters.
		0x2225 // 0x8161 of SJIS/WINDOWS-31J, converted to differently to/from ucs2
	};

	/**
	 * Test data of ujis. ujis consists of ASCII, JIS-Roman, JISX0201, JISX0208,
	 * JISX0212.
	 */
	public static final char[] UJIS_CHARS = new char[] {
		0xFF71, // halfwidth katakana letter A, 0x8EB1 of ujis, one of JISX0201.
		0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
		0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
		0x301C // wave dash, 0xA1C1 of ujis, convertion rule is different from ujis
	};

	/**
	 * Test data of eucjpms. ujis consists of ASCII, JIS-Roman, JISX0201,
	 * JISX0208, JISX0212, NEC special characters(row13)
	 */
	public static final char[] EUCJPMS_CHARS = new char[] {
		0xFF71, // halfwidth katakana letter A, 0x8EB1 of ujis, one of JISX0201.
		0x65E5, // CJK unified ideograph, 0xC6FC of ujis, one of JISX0208.
		0x7B5D, // CJK unified ideograph, 0xE4B882 of ujis, one of JISX0212
		0x3231, // parenthesized ideograph stok, 0x878A of WINDOWS-31J, one of NEC special characters(row13).
		0xFF5E // wave dash, 0xA1C1 of eucjpms, convertion rule is different from ujis
	};
}
