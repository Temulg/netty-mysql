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

package udentric.mysql.exceptions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.BatchUpdateException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import udentric.mysql.Messages;
import udentric.mysql.classic.jdbc.Connection;

public class SQLError {
	private SQLError() {
	}
	
	public static SQLException createSQLException(
		String message, String sqlState
	) {
		return createSQLException(message, sqlState, 0);
	}

	public static SQLException createSQLException(String message) {
		return new SQLException(message);
	}

	public static SQLException createSQLException(
		String message, String sqlState, Throwable cause
	) {
		SQLException sqlEx = createSQLException(
			message, sqlState, null
		);

		if (sqlEx.getCause() == null) {
			if (cause != null) {
				try {
					sqlEx.initCause(cause);
				} catch (Throwable t) {}
			}
		}
		return sqlEx;
	}

	public static SQLException createSQLException(
		String message, String sqlState, int vendorErrorCode
	) {
		return createSQLException(
			message, sqlState, vendorErrorCode, false
		);
	}

	public static SQLException createSQLException(
		String message, String sqlState, int vendorErrorCode,
		Throwable cause
	) {
		return createSQLException(
			message, sqlState, vendorErrorCode, false, cause
		);
	}

	public static SQLException createSQLException(
		String message, String sqlState, int vendorErrorCode,
		boolean isTransient
	) {
		return createSQLException(
			message, sqlState, vendorErrorCode, isTransient, null
		);
	}

	private static SQLException createExceptionForState(
		String message, String sqlState, int vendorErrorCode,
		boolean  isTransient
	) {
		int len = sqlState.length();

		if (len < 2) {
			return new SQLException(
				message, sqlState, vendorErrorCode
			);
		}

		switch (sqlState.substring(0, 2)) {
		case "08":
			if (isTransient)
				return new SQLTransientConnectionException(
					message, sqlState, vendorErrorCode
				);
			else
				return new SQLNonTransientConnectionException(
					message, sqlState, vendorErrorCode
				);
		case "22":
			return new SQLDataException(
				message, sqlState, vendorErrorCode
			);
		case "23":
			return new SQLIntegrityConstraintViolationException(
				message, sqlState, vendorErrorCode
			);
		case "40":
			return new MySQLTransactionRollbackException(
				message, sqlState, vendorErrorCode
			);
		case "42":
			return new SQLSyntaxErrorException(
				message, sqlState, vendorErrorCode
			);
		case "70":
			if (sqlState.startsWith("70100")) {
				return new MySQLQueryInterruptedException(
					message, sqlState, vendorErrorCode
				);
			} else
				break;
		}

		return new SQLException(
			message, sqlState, vendorErrorCode
		);
	}

	public static SQLException createSQLException(
		String message, String sqlState, int vendorErrorCode,
		boolean isTransient, Throwable cause
	) {

		try {
			SQLException sqlEx;

			if (sqlState != null) {
				sqlEx = createExceptionForState(
					message, sqlState, vendorErrorCode,
					isTransient
				);
			} else {
				sqlEx = new SQLException(
					message, sqlState, vendorErrorCode
				);
			}

			if (cause != null) {
				try {
					sqlEx.initCause(cause);
				} catch (Throwable t) {}
			}

			return sqlEx;
		} catch (Exception sqlEx) {
			return new SQLException(
				"Unable to create correct SQLException class "
				+ "instance, error class/codes may be "
				+ "incorrect. Reason: "
				+ stackTraceToString(sqlEx),
				MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR
			);
		}
	}

	public static SQLException createCommunicationsException(
		Connection conn, long lastPacketSentTimeMs,
		long lastPacketReceivedTimeMs, Exception underlyingException
	) {
		SQLException exToReturn = new CommunicationsException(
			conn, lastPacketSentTimeMs, lastPacketReceivedTimeMs,
			underlyingException
		);

		if (underlyingException != null) {
			try {
				exToReturn.initCause(underlyingException);
			} catch (Throwable t) {}
		}

		return exToReturn;
	}

	public static SQLException createCommunicationsException(
		String message, Throwable underlyingException
	) {
		SQLException exToReturn = new CommunicationsException(
			message, underlyingException
		);

		if (underlyingException != null) {
			try {
				exToReturn.initCause(underlyingException);
			} catch (Throwable t) {}
		}

		return exToReturn;
	}

	public static SQLException createBatchUpdateException(
		SQLException underlyingEx, long[] updateCounts
	) throws SQLException {
		return new BatchUpdateException(
			underlyingEx.getMessage(),
			underlyingEx.getSQLState(),
			underlyingEx.getErrorCode(),
			updateCounts, underlyingEx
		);
	}

	public static SQLException createSQLFeatureNotSupportedException() {
		return new SQLFeatureNotSupportedException();
	}

	public static SQLException createSQLFeatureNotSupportedException(
		String message, String sqlState
	) throws SQLException {
		return new SQLFeatureNotSupportedException(
			message, sqlState
		);
	}

	public static String stackTraceToString(Throwable ex) {
		StringWriter out = new StringWriter();
		PrintWriter printOut = new PrintWriter(out);

		printOut.append(Messages.getString("Util.1"));

		if (ex != null) {
			printOut.append(ex.getClass().getName());

			String message = ex.getMessage();

			if (message != null) {
				printOut.append(Messages.getString("Util.2"));
				printOut.append(message);
			}

			printOut.append(Messages.getString("Util.3"));
			ex.printStackTrace(printOut);
		}

		printOut.append(Messages.getString("Util.4"));
		printOut.flush();
		return out.getBuffer().toString();
	}
}
