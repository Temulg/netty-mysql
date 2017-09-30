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

package udentric.mysql.classic.dicta;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.Promise;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.SessionInfo;

public class CloseStatement implements Dictum {
	public CloseStatement(
		PreparedStatement stmt_, Promise<Packet.ServerAck> sp_
	) {
		stmt = stmt_;
		sp = sp_;
	}

	@Override
	public void emitClientMessage(ByteBuf dst, ChannelHandlerContext ctx) {
		dst.writeByte(OPCODE);
		dst.writeIntLE(stmt.getServerId());
	}

	@Override
	public void acceptServerMessage(ByteBuf src, ChannelHandlerContext ctx) {
		if (Packet.invalidSeqNum(ctx.channel(), src, 1))
			return;

		src.skipBytes(Packet.HEADER_SIZE);
		Channel ch = ctx.channel();
		SessionInfo si = Channels.sessionInfo(ch);

		int type = Packet.readInt1(src);
		switch (type) {
		case Packet.OK:
			try {
				Packet.ServerAck ack = new Packet.ServerAck(
					src, true, si.charset()
				);
				ch.attr(Channels.PSTMT_TRACKER).get().discard(
					stmt
				);
				Channels.discardActiveDictum(ch);
				sp.setSuccess(ack);
			} catch (Exception e) {
				Channels.discardActiveDictum(ch, e);
			}
			break;
		case Packet.ERR:
			Channels.discardActiveDictum(
				ch, Packet.parseError(src, si.charset())
			);
			return;
		default:
			Channels.discardActiveDictum(ch, new DecoderException(
				"unsupported packet type "
				+ Integer.toString(type)
			));
		}
	}

	@Override
	public void handleFailure(Throwable cause) {
		sp.setFailure(cause);
	}

	public static final int OPCODE = 25;

	private final PreparedStatement stmt;
	private final Promise<Packet.ServerAck> sp;
}
