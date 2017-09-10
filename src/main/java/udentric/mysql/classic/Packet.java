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

public class Packet {
	private Packet() {}

	public static int getLength(ByteBuf in) {
		return in.getMediumLE(in.readerIndex());
	}

	public static int getSeqNum(ByteBuf in) {
		return 0xff & in.getByte(in.readerIndex() + 3);
	}

	public static class OK	{
		public OK(ByteBuf msg, CharsetInfo.Entry charset) {
			rows = Fields.readLongLenenc(msg);
			insertId = Fields.readLongLenenc(msg);
			srvStatus = msg.readShortLE();
			warnCount = Fields.readInt2(msg);

			info = msg.readCharSequence(
				msg.readableBytes(), charset.javaCharset
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
