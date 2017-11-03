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

import java.sql.SQLException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.nio.charset.Charset;
import udentric.mysql.ErrorNumbers;

public class Packet {
	private Packet() {}

	public static int getLength(ByteBuf in) {
		return in.getMediumLE(in.readerIndex());
	}

	public static int getSeqNum(ByteBuf in) {
		return 0xff & in.getByte(in.readerIndex() + 3);
	}

	public static int nextSeqNum(int seqNum) {
		return (seqNum + 1) & 0xff;
	}

	public static int readInt1(ByteBuf in) {
		byte v = in.readByte();
		return (int)v & 0xff;
	}

	public static int readInt2(ByteBuf in) {
		short v = in.readShortLE();
		return (int)v & 0xffff;
	}

	public static int readIntLenenc(ByteBuf in) {
		int len = readInt1(in);
		if (len < 0xfb) {
			return len;
		}

		switch (len) {
		case 0xfb:
			return 0;
		case 0xfc:
			return readInt2(in);
		case 0xfd:
			return in.readMediumLE();
		case 0xfe:
			long llen = in.readLongLE();
			if (llen <= Integer.MAX_VALUE) {
				return (int)llen;
			}
		default:
			Channels.throwAny(makeError(
				ErrorNumbers.ER_MALFORMED_PACKET
			));
			return 0;
		}
	}

	public static long readLong2(ByteBuf in) {
		short v = in.readShortLE();
		return (long)v & 0xffff;
	}

	public static long readLongLenenc(ByteBuf in) {
		int len = readInt1(in);
		if (len < 0xfb) {
			return len;
		}

		switch (len) {
		case 0xfb:
			return 0;
		case 0xfc:
			return readLong2(in);
		case 0xfd:
			return in.readMediumLE();
		case 0xfe:
			return in.readLongLE();
		default:
			Channels.throwAny(makeError(
				ErrorNumbers.ER_MALFORMED_PACKET
			));
			return 0;
		}
	}

	public static String readStringNT(ByteBuf in, Charset cs) {
		int len = in.bytesBefore((byte)0);
		if (len < 0)
			Channels.throwAny(makeError(
				ErrorNumbers.ER_MALFORMED_PACKET
			));

		String rv = in.readCharSequence(len, cs).toString();
		in.skipBytes(1);
		return rv;
        }

	public static String readStringLenenc(ByteBuf in, Charset cs) {
		int len = readInt1(in);
		if (len < 0xfb) {
			return in.readCharSequence(len, cs).toString();
		}

		switch (len) {
		case 0xfb:
			return "";
		case 0xfc:
			return in.readCharSequence(
				readInt2(in), cs
			).toString();
		case 0xfd:
			return in.readCharSequence(
				in.readMediumLE(), cs
			).toString();
		case 0xfe:
			long llen = in.readLongLE();
			if (llen <= Integer.MAX_VALUE) {
				return in.readCharSequence(
					(int)llen, cs
				).toString();
			}
		default:
			Channels.throwAny(makeError(
				ErrorNumbers.ER_MALFORMED_PACKET
			));
			return "";
		}
	}

	public static void skipBytesLenenc(ByteBuf in) {
		int len = readInt1(in);
		if (len < 0xfb) {
			in.skipBytes(len);
			return;
		}

		switch (len) {
		case 0xfb:
			return;
		case 0xfc:
			in.skipBytes(readInt2(in));
			return;
		case 0xfd:
			in.skipBytes(in.readMediumLE());
			return;
		case 0xfe:
			long llen = in.readLongLE();
			if (llen <= Integer.MAX_VALUE) {
				in.skipBytes((int)llen);
				return;
			}
		}
		Channels.throwAny(makeError(
			ErrorNumbers.ER_MALFORMED_PACKET
		));
	}

	public static int writeIntLenenc(ByteBuf out, int val) {
		/* 251 is reserved for NULL */

		if (val == (val & 0xffffff)) {
			if (val < 0xfb) {
				out.writeByte(val);
				return 1;
			} else if (val < 0x10000) {
				out.writeByte(0xfc);
				out.writeShortLE(val);
				return 3;
			} else {
				out.writeByte(0xfd);
				out.writeMediumLE(val);
				return 4;
			}
		} else {
			out.writeByte(0xfe);
			out.writeLongLE((long)val & 0xffffffffL);
			return 9;
		}
	}

	public static boolean invalidSeqNum(
		Channel ch, ByteBuf src, int expected
	) {
		if (getSeqNum(src) != expected) {
			Channels.discardActiveDictum(ch, makeError(
				ErrorNumbers.ER_NET_PACKETS_OUT_OF_ORDER
			));
			return true;
		} else
			return false;
	}

	public static SQLException makeError(int errno) {
		String xOpen = ErrorNumbers.mysqlToSqlState(errno);
		return new SQLException(
			ErrorNumbers.get(xOpen), xOpen, errno
		);
	}

	public static SQLException makeError(int errno, String extra) {
		String xOpen = ErrorNumbers.mysqlToSqlState(errno);
		return new SQLException(
			String.format(
				"%s (%s)", ErrorNumbers.get(xOpen), extra
			), xOpen, errno
		);
	}

	public static SQLException makeErrorFromState(String state) {
		return new SQLException(
			ErrorNumbers.get(state), state
		);
	}

	public static SQLException parseError(
		ByteBuf msg, Charset cs
	) {
		int errno = readInt2(msg);
		String srvErrMsg = msg.readCharSequence(
			msg.readableBytes(), cs
		).toString();
		String xOpen;

		if (srvErrMsg.charAt(0) == '#') {
			if (srvErrMsg.length() > 6) {
				xOpen = srvErrMsg.substring(1, 6);
				srvErrMsg = srvErrMsg.substring(6);
				if ("HY000".equals(xOpen))
					xOpen = ErrorNumbers.mysqlToSqlState(
						errno
					);
			} else
				xOpen = ErrorNumbers.mysqlToSqlState(
					errno
				);
		} else
			xOpen = ErrorNumbers.mysqlToSqlState(errno);

		return new SQLException(srvErrMsg, xOpen, errno, null);
	}

	public static final int HEADER_SIZE = 4;
	public static final int OK = 0;
	public static final int FILE_REQUEST = 0xfb;
	public static final int EOF = 0xfe;
	public static final int ERR = 0xff;
}
