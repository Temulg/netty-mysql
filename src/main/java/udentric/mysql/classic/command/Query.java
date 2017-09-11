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

package udentric.mysql.classic.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.classic.CharsetInfo;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResponseConsumer;
import udentric.mysql.classic.ResponseType;
import udentric.mysql.classic.Session;


public class Query implements Any {
	public Query(String sql_, CharsetInfo.Entry charset_) {
		sql = sql_;
		charset = charset_;
	}

	public Query withResponseConsumer(ResponseConsumer rc_) {
		rc = rc_;
		return this;
	}

	@Override
	public ChannelPromise channelPromise() {
		return chp;
	}

	@Override
	public Query withChannelPromise(ChannelPromise chp_) {
		chp = chp_;
		return this;
	}

	@Override
	public void encode(ByteBuf dst, Session ss) {
		dst.writeByte(OPCODE);
		dst.writeCharSequence(sql, charset.javaCharset);
	}

	@Override
	public void handleReply(
		ByteBuf src, Session ss, ChannelHandlerContext ctx
	) {
		int nextSeqNum = Packet.getSeqNum(src);
		src.skipBytes(Packet.HEADER_SIZE);

		int type = src.getByte(src.readerIndex()) & 0xff;

		System.err.format("--7- resp reply type %x\n", type);
		switch (type) {
		case ResponseType.OK:
			src.skipBytes(1);
			try {
				Packet.Ok ok = new Packet.Ok(src, charset);
				ss.discardCommand();
				if (rc != null)
					rc.onSuccess(ok);
				if (chp != null)
					chp.setSuccess();
			} catch (Exception e) {
				ss.discardCommand(e);
			}
			break;
		case ResponseType.ERR:
			src.skipBytes(1);
			ss.discardCommand(Packet.parseError(src, charset));
			return;
		default:
			StringBuilder sb = new StringBuilder(
				"-a7- resultset\n"
			);
			ByteBufUtil.appendPrettyHexDump(sb, src);
			System.err.println(sb);
			return;
		}
	}

	@Override
	public void handleFailure(Throwable cause) {
		if (rc != null)
			rc.onFailure(cause);
		if (chp != null)
			chp.setFailure(cause);
	}

	public static final int OPCODE = 3;

	private final String sql;
	private final CharsetInfo.Entry charset;
	private ResponseConsumer rc;
	private ChannelPromise chp;
}
