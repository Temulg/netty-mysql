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
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.Function;
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
		defaultTimeZone = defaultTimeZone_ != null
		? defaultTimeZone_ : TimeZone.getDefault();
		serverSupportsFractionalSecond = serverSupportsFractionalSecond_;
		exceptionInterceptor = exceptionInterceptor_;
	}

	public CharSequence normalize() throws SQLException {
		if (srcLastPos == 0)
			return null;

		int pos = src.indexOf(CHR_BEGIN_SUBEXPR);
		if (pos < 0)
			return src;

		pos = src.indexOf(CHR_END_SUBEXPR, pos);
		if (pos < 0)
			return src;

		parseSrc();
		return rootExpr.getBuf();
	}

	@Override
	protected CharSequence updateSubExpr(CharSequence seq) {
		int pos = skipWhitespaceFwd(seq, 1);
		if (pos >= seq.length())
			return seq;

		Matcher m;

		switch (Character.toLowerCase(seq.charAt(pos))) {
		case 'e': // escape
			break;
		case 'f': //fn
			return updateFnSubExpr(seq);
		case 'd': //d
			m = EXPR_DATE.matcher(seq);
			if (!m.matches())
				break;
			return formatDate(m.group(1), seq);
		case 't': //t || ts
			m = EXPR_TIME.matcher(seq);
			if (!m.matches())
				break;

			String s = m.group(1);

			if (s != null && !s.isEmpty()) {
				return formatTimestamp(m.group(2), seq);
			} else {
				return formatTime(m.group(2), seq);
			}
		case 'c': // call
			break; 
		case '?': //?=call
			break;
		}
		return seq;
	}

	private CharSequence updateFnSubExpr(CharSequence seq) {
		Matcher m = EXPR_FN_ANY.matcher(seq);
		if (!m.matches())
			return seq;

		String fn = m.group(1);


		m = EXPR_FN_CONVERT.matcher(fn);

		if (!m.matches())
			return fn;


		String type = m.group(3);
		return JDBC_CONVERT_TO_MYSQL_TYPE_MAP.getOrDefault(
			type.toUpperCase(Locale.ENGLISH),
			expr -> {
				return seq;
			}
		).apply(m.group(1));
	}

	private CharSequence formatDate(CharSequence val, CharSequence orig) {
		return LocalDate.parse(
			val, DateTimeFormatter.ISO_LOCAL_DATE
		).format(MYSQL_DATE);
	}

	private CharSequence formatTimestamp(
		CharSequence val, CharSequence orig
	) {
		Timestamp ts = Timestamp.valueOf(val.toString());

		return ZonedDateTime.of(
			ts.toLocalDateTime(), defaultTimeZone.toZoneId()
		).format(
			serverSupportsFractionalSecond
			? MYSQL_DATE_TIME_FRAC : MYSQL_DATE_TIME
		);
	}

	private CharSequence formatTime(CharSequence val, CharSequence orig) {
		return LocalTime.parse(
			val, DateTimeFormatter.ISO_LOCAL_TIME
		).format(
			serverSupportsFractionalSecond
			? MYSQL_TIME_FRAC : MYSQL_TIME
		);
	}

	public static int skipWhitespaceFwd(CharSequence seq, int first) {
		int pos = first;
		for (; pos < seq.length(); pos++) {
			if (!Character.isWhitespace(seq.charAt(pos)))
				break;
		}

		return pos;
	}

	public final static byte USES_VARIABLES_FALSE = 0;
	public final static byte USES_VARIABLES_TRUE = 1;
	public final static byte USES_VARIABLES_UNKNOWN = -1;

	private static final Pattern EXPR_FN_CONVERT = Pattern.compile(
		"^convert\\s*\\((.+?)\\s*,\\s*(sql_)?(.+?)\\s*\\)$",
		Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
	);

	private static final Pattern EXPR_FN_ANY = Pattern.compile(
		"^\\{\\s*fn\\s+(.*?)\\s*\\}$",
		Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
	);

	private static final Pattern EXPR_DATE = Pattern.compile(
		"^\\{\\s*d\\s+'(.*?)'\\s*\\}$",
		Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE	
	);

	private static final Pattern EXPR_TIME = Pattern.compile(
		"^\\{\\s*t(s?)\\s+'(.*?)'\\s*\\}$",
		Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE	
	);

	private static final DateTimeFormatter MYSQL_DATE
	= new DateTimeFormatterBuilder().appendLiteral('\'').append(
		DateTimeFormatter.ISO_LOCAL_DATE
	).appendLiteral('\'').toFormatter(Locale.US);

	private static final DateTimeFormatter MYSQL_TIME
	= new DateTimeFormatterBuilder().appendLiteral('\'').appendValue(
		ChronoField.HOUR_OF_DAY, 2
	).appendLiteral(':').appendValue(
		ChronoField.MINUTE_OF_HOUR, 2
	).optionalStart().appendLiteral(':').appendValue(
		ChronoField.SECOND_OF_MINUTE, 2
	).appendLiteral('\'').toFormatter(Locale.US);

	private static final DateTimeFormatter MYSQL_TIME_FRAC
	= new DateTimeFormatterBuilder().appendLiteral('\'').append(
		DateTimeFormatter.ISO_LOCAL_TIME
	).appendLiteral('\'').toFormatter(Locale.US);

	private static final DateTimeFormatter MYSQL_DATE_TIME
	= new DateTimeFormatterBuilder().appendLiteral('\'').append(
		DateTimeFormatter.ISO_LOCAL_DATE
	).appendLiteral(' ').appendValue(
		ChronoField.HOUR_OF_DAY, 2
	).appendLiteral(':').appendValue(
		ChronoField.MINUTE_OF_HOUR, 2
	).optionalStart().appendLiteral(':').appendValue(
		ChronoField.SECOND_OF_MINUTE, 2
	).appendLiteral('\'').toFormatter(Locale.US);

	private static final DateTimeFormatter MYSQL_DATE_TIME_FRAC
	= new DateTimeFormatterBuilder().appendLiteral('\'').append(
		DateTimeFormatter.ISO_LOCAL_DATE
	).appendLiteral(' ').append(
		DateTimeFormatter.ISO_LOCAL_TIME
	).appendLiteral('\'').toFormatter(Locale.US);

	private static final ImmutableMap<
		String, Function<CharSequence, CharSequence>
	> JDBC_CONVERT_TO_MYSQL_TYPE_MAP = ImmutableMap.<
		String, Function<CharSequence, CharSequence>
	>builder().put("BIGINT", expr -> {
		return "0 + " + expr;
	}).put("BINARY", expr -> {
		return "CAST(" + expr + " AS BINARY)";
	}).put("BIT", expr -> {
		return "0 + " + expr;
	}).put("CHAR", expr -> {
		return "CAST(" + expr + " AS CHAR)";
	}).put("DATE", expr -> {
		return "CAST(" + expr + " AS DATE)";
	}).put("DECIMAL", expr -> {
		return "0.0 + " + expr;
	}).put("DOUBLE", expr -> {
		return "0.0 + " + expr;
	}).put("FLOAT", expr -> {
		return "0.0 + " + expr;
	}).put("INTEGER", expr -> {
		return "0 + " + expr;
	}).put("LONGVARBINARY", expr -> {
		return "CAST(" + expr + " AS BINARY)";
	}).put("LONGVARCHAR", expr -> {
		return "CONCAT(" + expr + ")";
	}).put("REAL", expr -> {
		return "0.0 + " + expr;
	}).put("SMALLINT", expr -> {
		return "CONCAT(" + expr + ")";
	}).put("TIME", expr -> {
		return "CAST(" + expr + " AS TIME)";
	}).put("TIMESTAMP", expr -> {
		return "CAST(" + expr + " AS DATETIME)";
	}).put("TINYINT", expr -> {
		return "CONCAT(" + expr + ")";
	}).put("VARBINARY", expr -> {
		return "CAST(" + expr + " AS BINARY)";
	}).put("VARCHAR", expr -> {
		return "CONCAT(" + expr + ")";
	}).build();

	private final TimeZone defaultTimeZone;
	private final boolean serverSupportsFractionalSecond;
	private final ExceptionInterceptor exceptionInterceptor;
}
