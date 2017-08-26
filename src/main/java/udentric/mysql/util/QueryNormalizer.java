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
 * May contain portions of MySQL Connector/J implementation
 *
 * Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * The MySQL Connector/J is licensed under the terms of the GPLv2
 * <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL
 * Connectors. There are special exceptions to the terms and conditions of
 * the GPLv2 as it is applied to this software, see the FOSS License Exception
 * <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
 */

package udentric.mysql.util;

import java.sql.SQLException;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;

public class QueryNormalizer extends QueryTokenizer {
	public QueryNormalizer(
		String s, TimeZone defaultTimeZone_,
		boolean serverSupportsFractionalSecond_,
		ExceptionInterceptor exceptionInterceptor_
	) {
		super(s);
		defaultTimeZone = defaultTimeZone_;
		serverSupportsFractionalSecond = serverSupportsFractionalSecond_;
		exceptionInterceptor = exceptionInterceptor_;
	}

	public CharSequence normalize() throws SQLException {
		if (srcLastPos == 0) {
			return null;
		}

		parseSrc();
		return rootExpr.getBuf();
	}

	@Override
	protected CharSequence updateSubExpr(CharSequence seq) {
		Matcher m = EXPR_CLASSIFIER.matcher(seq);

		if (!m.matches())
			return seq;

		String type = m.group(1).toLowerCase();

		switch (type) {
		case "escape":
		case "fn":
			return updateFnSubExpr(seq);
		case "d":
		case "ts":
		case "t":
		case "call":
		case "?=call":
		default:
			return seq;
		}
	}

	private CharSequence updateFnSubExpr(CharSequence seq) {
		Matcher m = EXPR_FN_CONVERT.matcher(seq);

		if (!m.matches())
			return seq;

		String type = m.group(1).toLowerCase();
		return seq;
	}

	public final static byte USES_VARIABLES_FALSE = 0;
	public final static byte USES_VARIABLES_TRUE = 1;
	public final static byte USES_VARIABLES_UNKNOWN = -1;

	private static final Pattern EXPR_CLASSIFIER = Pattern.compile(
		"^\\{\\s*([acdeflnpst?=]+)",
		Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
	);
	private static final Pattern EXPR_FN_CONVERT = Pattern.compile(
		"^\\{\\s*fn\\s+convert\\s+",
		Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
	);

	private static final ImmutableMap<
		String, String
	> JDBC_CONVERT_TO_MYSQL_TYPE_MAP = ImmutableMap.<
		String, String
	>builder().put(
		"BIGINT", "0 + ?"
	).put(
		"BINARY", "BINARY"
	).put(
		"BIT", "0 + ?"
	).put(
		"CHAR", "CHAR"
	).put(
		"DATE", "DATE"
	).put(
		"DECIMAL", "0.0 + ?"
	).put(
		"DOUBLE", "0.0 + ?"
	).put(
		"FLOAT", "0.0 + ?"
	).put(
		"INTEGER", "0 + ?"
	).put(
		"LONGVARBINARY", "BINARY"
	).put(
		"LONGVARCHAR", "CONCAT(?)"
	).put(
		"REAL", "0.0 + ?"
	).put(
		"SMALLINT", "CONCAT(?)"
	).put(
		"TIME", "TIME"
	).put(
		"TIMESTAMP", "DATETIME"
	).put(
		"TINYINT", "CONCAT(?)"
	).put(
		"VARBINARY", "BINARY"
	).put(
		"VARCHAR", "CONCAT(?)"
	).build();

	private final TimeZone defaultTimeZone;
	private final boolean serverSupportsFractionalSecond;
	private final ExceptionInterceptor exceptionInterceptor;
}
