/*
 * Copyright (c) 2017 Alex Dubov <oakad@yahoo.com>
 *
 * This file is made available under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package udentric.mysql.util;

import java.util.TimeZone;

import com.google.common.collect.ImmutableMap;

public class QueryNormalizer {
	private QueryNormalizer() {}
/*
	public static final Object escapeSQL(
		String sql, TimeZone defaultTimeZone,
		boolean serverSupportsFractionalSecond,
		ExceptionInterceptor exceptionInterceptor
	) throws java.sql.SQLException {
		boolean replaceEscapeSequence = false;
		String escapeSequence = null;

		if (sql == null)
			return null;

		int beginBrace = sql.indexOf('{');
		int nextEndBrace = (beginBrace == -1) ? (-1) : sql.indexOf('}', beginBrace);

		if (nextEndBrace == -1)
			return sql;

		StringBuilder newSql = new StringBuilder();
		EscapeTokenizer escapeTokenizer = new EscapeTokenizer(sql);

		byte usesVariables = StatementImpl.USES_VARIABLES_FALSE;
		boolean callingStoredFunction = false;

		while (escapeTokenizer.hasMoreTokens()) {
			String token = escapeTokenizer.nextToken();

			if (token.length() != 0) {
				if (token.charAt(0) == '{') {
					if (!token.endsWith("}")) {
						throw SQLError.createSQLException(Messages.getString("EscapeProcessor.0", new Object[] { token }), exceptionInterceptor);
					}

					if (token.length() > 2) {
						int nestedBrace = token.indexOf('{', 2);

						if (nestedBrace != -1) {
							StringBuilder buf = new StringBuilder(token.substring(0, 1));

							Object remainingResults = escapeSQL(
								token.substring(1, token.length() - 1),
								defaultTimeZone,
								serverSupportsFractionalSecond,
								exceptionInterceptor
							);

							String remaining = null;

							if (remainingResults instanceof String) {
								remaining = (String) remainingResults;
							} else {
								remaining = ((EscapeProcessorResult) remainingResults).escapedSql;

								if (usesVariables != StatementImpl.USES_VARIABLES_TRUE) {
									usesVariables = ((EscapeProcessorResult) remainingResults).usesVariables;
								}
							}

							buf.append(remaining);

							buf.append('}');

							token = buf.toString();
						}
					}

					String collapsedToken = removeWhitespace(token);

					if (StringUtils.startsWithIgnoreCase(collapsedToken, "{escape")) {
						try {
							StringTokenizer st = new StringTokenizer(token, " '");
							st.nextToken(); // eat the "escape" token
							escapeSequence = st.nextToken();

							if (escapeSequence.length() < 3) {
								newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
							} else {
								escapeSequence = escapeSequence.substring(1, escapeSequence.length() - 1);
								replaceEscapeSequence = true;
							}
						} catch (java.util.NoSuchElementException e) {
							newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
						}
					} else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{fn")) {
						int startPos = token.toLowerCase().indexOf("fn ") + 3;
						int endPos = token.length() - 1; // no }

						String fnToken = token.substring(startPos, endPos);

						if (StringUtils.startsWithIgnoreCaseAndWs(fnToken, "convert")) {
							newSql.append(processConvertToken(fnToken, exceptionInterceptor));
						} else {
							newSql.append(fnToken);
						}
					} else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{d")) {
						int startPos = token.indexOf('\'') + 1;
						int endPos = token.lastIndexOf('\''); // no }

						if ((startPos == -1) || (endPos == -1)) {
							newSql.append(token); // it's just part of the query, push possible syntax errors onto server's shoulders
						} else {
							String argument = token.substring(startPos, endPos);

							try {
								StringTokenizer st = new StringTokenizer(argument, " -");
								String year4 = st.nextToken();
								String month2 = st.nextToken();
								String day2 = st.nextToken();
								String dateString = "'" + year4 + "-" + month2 + "-" + day2 + "'";
								newSql.append(dateString);
							} catch (java.util.NoSuchElementException e) {
								throw SQLError.createSQLException(Messages.getString("EscapeProcessor.1", new Object[] { argument }),
									MysqlErrorNumbers.SQL_STATE_SYNTAX_ERROR, exceptionInterceptor);
							}
						}
					} else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{ts")) {
						processTimestampToken(defaultTimeZone, newSql, token, serverSupportsFractionalSecond, exceptionInterceptor);
					} else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{t")) {
						processTimeToken(newSql, token, serverSupportsFractionalSecond, exceptionInterceptor);
					} else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{call") || StringUtils.startsWithIgnoreCase(collapsedToken, "{?=call")) {

						int startPos = StringUtils.indexOfIgnoreCase(token, "CALL") + 5;
						int endPos = token.length() - 1;

						if (StringUtils.startsWithIgnoreCase(collapsedToken, "{?=call")) {
							callingStoredFunction = true;
							newSql.append("SELECT ");
							newSql.append(token.substring(startPos, endPos));
						} else {
							callingStoredFunction = false;
							newSql.append("CALL ");
							newSql.append(token.substring(startPos, endPos));
						}

						for (int i = endPos - 1; i >= startPos; i--) {
							char c = token.charAt(i);

							if (Character.isWhitespace(c)) {
								continue;
							}

							if (c != ')') {
								newSql.append("()"); // handle no-parenthesis no-arg call not supported by MySQL parser
							}

							break;
						}
					} else if (StringUtils.startsWithIgnoreCase(collapsedToken, "{oj")) {
						newSql.append(token);
					} else {
						newSql.append(token);
					}
				} else {
					newSql.append(token); // it's just part of the query
				}
			}
		}

		String escapedSql = newSql.toString();

		if (replaceEscapeSequence) {
			String currentSql = escapedSql;

			while (currentSql.indexOf(escapeSequence) != -1) {
				int escapePos = currentSql.indexOf(escapeSequence);
				String lhs = currentSql.substring(0, escapePos);
				String rhs = currentSql.substring(escapePos + 1, currentSql.length());
				currentSql = lhs + "\\" + rhs;
			}

			escapedSql = currentSql;
		}

		EscapeProcessorResult epr = new EscapeProcessorResult();
		epr.escapedSql = escapedSql;
		epr.callingStoredFunction = callingStoredFunction;

		if (usesVariables != StatementImpl.USES_VARIABLES_TRUE) {
			if (escapeTokenizer.sawVariableUse()) {
				epr.usesVariables = StatementImpl.USES_VARIABLES_TRUE;
			} else {
				epr.usesVariables = StatementImpl.USES_VARIABLES_FALSE;
			}
		}

		return epr;
	}
*/
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
}
