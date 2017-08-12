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

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

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

	public static CharSequence readStringNT(ByteBuf in) {
		int len = in.bytesBefore((byte)0);
		if (len < 0)
			throw new DecoderException(
				"malformed server handshake message"
			);

		CharSequence cs = in.readCharSequence(
			len, StandardCharsets.ISO_8859_1
		);
		in.skipBytes(1);
		return cs;
	}

	public static byte[] readBytes(ByteBuf in, int len) {
		byte[] rv = new byte[len];
		in.readBytes(rv);
		return rv;
	}
}
