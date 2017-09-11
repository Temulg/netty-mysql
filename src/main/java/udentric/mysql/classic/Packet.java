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

package udentric.mysql.classic;

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import udentric.mysql.Messages;
import udentric.mysql.exceptions.ClosedOnExpiredPasswordException;
import udentric.mysql.exceptions.DataTruncationException;
import udentric.mysql.exceptions.ExceptionFactory;
import udentric.mysql.exceptions.MysqlErrorNumbers;
import udentric.mysql.exceptions.PasswordExpiredException;

public class Packet {
	private Packet() {}

	public static int getLength(ByteBuf in) {
		return in.getMediumLE(in.readerIndex());
	}

	public static int getSeqNum(ByteBuf in) {
		return 0xff & in.getByte(in.readerIndex() + 3);
	}

	public static Exception parseError(
		ByteBuf msg, CharsetInfo.Entry charset
	) {
		int errno = Fields.readInt2(msg);
		String srvErrMsg = msg.readCharSequence(
			msg.readableBytes(), charset.javaCharset
		).toString();
		String xOpen = MysqlErrorNumbers.SQL_STATE_CLI_SPECIFIC_CONDITION;

		if (srvErrMsg.charAt(0) == '#') {
			if (srvErrMsg.length() > 6) {
				xOpen = srvErrMsg.substring(1, 6);
				srvErrMsg = srvErrMsg.substring(6);
				if ("HY000".equals(xOpen))
					xOpen = MysqlErrorNumbers.mysqlToSqlState(
						errno
					);
			} else
				xOpen = MysqlErrorNumbers.mysqlToSqlState(
					errno
				);
		} else
			xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);
		
		StringBuilder errBuf = new StringBuilder(
			MysqlErrorNumbers.get(xOpen)
		).append(
			Messages.getString("Protocol.0")
		).append(srvErrMsg).append('"');

		if (xOpen.startsWith("22"))
			return new DataTruncationException(
				errBuf.toString(), 0, true, false, 0, 0,
				errno
			);
		
		switch (errno) {
		case MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD:
			return ExceptionFactory.createException(
				PasswordExpiredException.class,
				errBuf.toString()
			);
		case MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN:
			return ExceptionFactory.createException(
				ClosedOnExpiredPasswordException.class,
				errBuf.toString()
			);
		default:
			return ExceptionFactory.createException(
				errBuf.toString(), xOpen, errno, false, null
			);
		}
	}

	public static class Ok	{
		public Ok(ByteBuf msg, CharsetInfo.Entry charset) {
			rows = Fields.readLongLenenc(msg);
			insertId = Fields.readLongLenenc(msg);
			srvStatus = msg.readShortLE();
			warnCount = Fields.readInt2(msg);

			info = msg.readCharSequence(
				msg.readableBytes(), charset.javaCharset
			).toString();
		}

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this).add(
				"rows", rows
			).add(
				"insertId", insertId
			).add(
				"srvStatus", srvStatus
			).add(
				"warnCount", warnCount
			).add(
				"info", info
			).toString();
		}

		public final long rows;
		public final long insertId;
		public final short srvStatus;
		public final int warnCount;
		public final String info;
	}

	public static final int HEADER_SIZE = 4;
}
