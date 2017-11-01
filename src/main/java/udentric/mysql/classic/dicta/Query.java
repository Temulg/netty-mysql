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
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerStatus;
import udentric.mysql.classic.SessionInfo;


public class Query implements Dictum {
	public Query(String sql_, ResultSetConsumer rsc_) {
		sql = sql_;
		rsc = rsc_;
	}

	@Override
	public boolean emitClientMessage(ByteBuf dst, ChannelHandlerContext ctx) {
		dst.writeByte(OPCODE);
		dst.writeCharSequence(
			sql, Channels.sessionInfo(ctx.channel()).charset()
		);
		return false;
	}

	@Override
	public void acceptServerMessage(ByteBuf src, ChannelHandlerContext ctx) {
		if (Packet.invalidSeqNum(ctx.channel(), src, 1))
			return;

		int seqNum = Packet.getSeqNum(src);
		Channel ch = ctx.channel();
		SessionInfo si = Channels.sessionInfo(ch);

		int type = src.getByte(
			src.readerIndex() + Packet.HEADER_SIZE
		) & 0xff;

		switch (type) {
		case Packet.OK:
			src.skipBytes(Packet.HEADER_SIZE + 1);
			try {
				Packet.ServerAck ack = new Packet.ServerAck(
					src, true, si.charset()
				);
				if (ServerStatus.MORE_RESULTS_EXISTS.get(
					ack.srvStatus
				)) {
					ch.attr(Channels.ACTIVE_DICTUM).set(
						new TextResultSet(
							seqNum, rsc
						)
					);
					rsc.acceptAck(ack, false);
				} else {
					Channels.discardActiveDictum(ch);
					rsc.acceptAck(ack, true);
				}
			} catch (Exception e) {
				Channels.discardActiveDictum(ch, e);
			}
			break;
		case Packet.ERR:
			src.skipBytes(Packet.HEADER_SIZE + 1);
			Channels.discardActiveDictum(
				ch, Packet.parseError(src, si.charset())
			);
			return;
		default:
			src.skipBytes(Packet.HEADER_SIZE);
			ch.attr(Channels.ACTIVE_DICTUM).set(new TextResultSet(
				Packet.readIntLenenc(src), seqNum, rsc
			));
		}
	}

	@Override
	public void handleFailure(Throwable cause) {
		rsc.acceptFailure(cause);
	}

	public static final int OPCODE = 3;

	private final String sql;
	private final ResultSetConsumer rsc;
}
