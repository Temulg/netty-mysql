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
import io.netty.util.concurrent.Promise;
import udentric.mysql.ErrorNumbers;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ColumnDefinition;
import udentric.mysql.classic.Field;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.SessionInfo;

public class PrepareStatement implements Dictum {
	public PrepareStatement(String sql_, Promise<PreparedStatement> psp_) {
		sql = sql_;
		psp = psp_;
		state = this::stmtPrepared;
		lastSeqNum = 1;
	}

	@Override
	public void emitClientMessage(
		ByteBuf dst, ChannelHandlerContext ctx
	) {
		dst.writeByte(OPCODE);
		dst.writeCharSequence(
			sql, Channels.sessionInfo(ctx.channel()).charset()
		);
	}

	@Override
	public void acceptServerMessage(
		ByteBuf src, ChannelHandlerContext ctx
	) {
		lastSeqNum = Packet.nextSeqNum(lastSeqNum);
		if (Packet.invalidSeqNum(ctx.channel(), src, lastSeqNum))
			return;

		SessionInfo si = Channels.sessionInfo(ctx.channel());

		state.accept(src, ctx, si);
	}

	private void stmtPrepared(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		Channel ch = ctx.channel();
		int type = src.getByte(
			src.readerIndex() + Packet.HEADER_SIZE
		) & 0xff;

		switch (type) {
		case Packet.OK:
			src.skipBytes(Packet.HEADER_SIZE + 1);
			try {
				stmtId = src.readIntLE();
				colDef = new ColumnDefinition(
					Packet.readInt2(src)
				);
				paramDef = new ColumnDefinition(
					Packet.readInt2(src)
				);
				src.skipBytes(1);
				warnCount = Packet.readInt2(src);

				if (!paramDef.hasAllFields())
					state = this::receiveParamDef;
				else if (!colDef.hasAllFields())
					state = this::receiveColDef;
				else
					completePreparation(ch);
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
		}
	}

	private void receiveParamDef(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		Channel ch = ctx.channel();

		if (!paramDef.hasAllFields()) {
			try {
				src.skipBytes(Packet.HEADER_SIZE);
				paramDef.appendField(
					new Field(src, si.charset())
				);
			} catch (Exception e) {
				Channels.discardActiveDictum(ch, e);
			}
		} else if (si.expectEof()) {
			src.skipBytes(Packet.HEADER_SIZE);
			if ((
				Packet.EOF != Packet.readInt1(src)
			) || (src.readableBytes() != 4)) {
				Channels.discardActiveDictum(
					ch, Packet.makeError(
						ErrorNumbers.ER_MALFORMED_PACKET
					)
				);
			} else {
				src.skipBytes(4);
				if (colDef.hasAllFields())
					completePreparation(ch);
				else
					state = this::receiveColDef;
			}
		} else {
			if (colDef.hasAllFields())
				completePreparation(ch);
			else
				state = this::receiveColDef;

			receiveColDef(src, ctx, si);
		}
	}

	private void receiveColDef(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		Channel ch = ctx.channel();

		if (!colDef.hasAllFields()) {
			try {
				src.skipBytes(Packet.HEADER_SIZE);
				colDef.appendField(
					new Field(src, si.charset())
				);
			} catch (Exception e) {
				Channels.discardActiveDictum(ch, e);
			}
		} else if (si.expectEof()) {
			src.skipBytes(Packet.HEADER_SIZE);
			if ((
				Packet.EOF != Packet.readInt1(src)
			) || (src.readableBytes() != 4)) {
				Channels.discardActiveDictum(
					ch, Packet.makeError(
						ErrorNumbers.ER_MALFORMED_PACKET
					)
				);
			} else {
				src.skipBytes(4);
				completePreparation(ch);
			}
		} else {
			completePreparation(ch);
		}
	}

	private void completePreparation(Channel ch) {
		Channels.discardActiveDictum(ch);
		ch.attr(Channels.PSTMT_TRACKER).get().completePrepare(
			sql, stmtId, paramDef, colDef
		);
	}

	@Override
	public void handleFailure(Throwable cause) {
		psp.setFailure(cause);
	}

	public static final int OPCODE = 22;

	private final String sql;
	private final Promise<PreparedStatement> psp;
	protected ServerMessageConsumer state;
	protected ColumnDefinition paramDef;
	protected ColumnDefinition colDef;
	protected int stmtId;
	protected int warnCount;
	protected int lastSeqNum;
}