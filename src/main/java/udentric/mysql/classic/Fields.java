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

package udentric.mysql.classic;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.util.ByteString;

public class Fields {
	private Fields() {}

	public static int readInt1(ByteBuf in) {
		byte v = in.readByte();
		return (int)v & 0xff;
	}

	public static int readInt2(ByteBuf in) {
		short v = in.readShortLE();
		return (int)v & 0xffff;
	}

	public static long readLong1(ByteBuf in) {
		byte v = in.readByte();
		return (long)v & 0xff;
	}

	public static long readLong2(ByteBuf in) {
		short v = in.readShortLE();
		return (long)v & 0xffff;
	}

	public static long readLong4(ByteBuf in) {
		int v = in.readIntLE();
		return (long)v & 0xffffffffL;
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
			throw new DecoderException("malformed packet");
		}
	}

	public static ByteString readStringNT(ByteBuf in) {
		int len = in.bytesBefore((byte)0);
		if (len < 0)
			throw new DecoderException("malformed packet");

		ByteString rv = new ByteString(in, len);
		in.skipBytes(1);
		return rv;
	}

	public static byte[] readBytes(ByteBuf in, int len) {
		byte[] rv = new byte[len];
		if (len > 0)
			in.readBytes(rv);

		return rv;
	}

	public static byte[] readBytesNT(ByteBuf in) {
		int len = in.bytesBefore((byte)0);
		if (len < 0)
			throw new DecoderException("malformed packet");

		byte[] rv = new byte[len];
		in.readBytes(rv);
		in.skipBytes(1);
		return rv;
	}

	public static ByteString readStringLenenc(ByteBuf in) {
		int len = readInt1(in);
		if (len < 0xfb) {
			return new ByteString(in, len);
		}

		switch (len) {
		case 0xfb:
			return null;
		case 0xfc:
			len = readInt2(in);
			break;
		case 0xfd:
			len = in.readMediumLE();
			break;
		case 0xfe:
			len = Math.toIntExact(in.readLongLE());
			break;
		default:
			throw new DecoderException("malformed packet");
		}

		return new ByteString(in, len);
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

	public static int writeLongLenenc(ByteBuf out, long val) {
		/* 251 is reserved for NULL */

		if (val == (val & 0xffffffL)) {
			if (val < 0xfbL) {
				out.writeByte((int)val);
				return 1;
  			} else if (val < 0x10000L) {
				out.writeByte(0xfc);
				out.writeShortLE((int)val);
				return 3;
			} else {
				out.writeByte(0xfd);
				out.writeMediumLE((int)val);
				return 4;
			}
		} else {
			out.writeByte(0xfe);
			out.writeLongLE(val);
			return 9;
		}
	}
}
