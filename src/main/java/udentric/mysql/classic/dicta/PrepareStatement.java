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
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.FieldSetImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.SessionInfo;

public class PrepareStatement implements Dictum {
	public PrepareStatement(String sql_, Promise<PreparedStatement> psp_) {
		sql = sql_;
		psp = psp_;
		state = this::stmtPrepared;
		lastSeqNum = 0;
	}

	@Override
	public boolean emitClientMessage(
		ByteBuf dst, ChannelHandlerContext ctx
	) {
		dst.writeByte(OPCODE);
		dst.writeCharSequence(
			sql, Channels.sessionInfo(ctx.channel()).charset()
		);
		return false;
	}

	@Override
	public void acceptServerMessage(
		ByteBuf src, ChannelHandlerContext ctx
	) {
		lastSeqNum = Packet.nextSeqNum(lastSeqNum);
		if (Packet.invalidSeqNum(ctx.channel(), src, lastSeqNum)) {
			return;
		}

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
				columns = new FieldSetImpl(
					Packet.readInt2(src)
				);
				parameters = new FieldSetImpl(
					Packet.readInt2(src)
				);
				src.skipBytes(1);
				warnCount = Packet.readInt2(src);

				if (parameters.size() > 0)
					state = this::receiveParamDef;
				else if (columns.size() > 0)
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

		try {
			src.skipBytes(Packet.HEADER_SIZE);
			parameters.set(
				paramFieldPos, 
				new FieldImpl(src, si.charset())
			);
			paramFieldPos++;
			if (paramFieldPos == parameters.size()) {
				if (si.expectEof())
					state = this::finishedWithParamDef;
				else if (columns.size() > 0)
					state = this::receiveColDef;
				else
					completePreparation(ch);
			}
		} catch (Exception e) {
			Channels.discardActiveDictum(ch, e);
		}		
	}

	private void finishedWithParamDef(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		Channel ch = ctx.channel();
		src.skipBytes(Packet.HEADER_SIZE);

		if ((
			Packet.EOF == Packet.readInt1(src)
		) || (src.readableBytes() == 4)) {
			src.skipBytes(4);

			if (columns.size() > 0)
				state = this::receiveColDef;
			else
				completePreparation(ch);
		} else {
			Channels.discardActiveDictum(
				ch, Packet.makeError(
					ErrorNumbers.ER_MALFORMED_PACKET
				)
			);
		}
	}

	private void receiveColDef(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		Channel ch = ctx.channel();

		try {
			src.skipBytes(Packet.HEADER_SIZE);
			columns.set(
				colFieldPos,
				new FieldImpl(src, si.charset())
			);
			colFieldPos++;
			if (colFieldPos == columns.size()) {
				if (si.expectEof())
					state = this::finishedWithColDef;
				else
					completePreparation(ch);
			}
		} catch (Exception e) {
			Channels.discardActiveDictum(ch, e);
		}
	}

	private void finishedWithColDef(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		Channel ch = ctx.channel();
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
	}

	private void completePreparation(Channel ch) {
		Channels.discardActiveDictum(ch);
		ch.attr(Channels.PSTMT_TRACKER).get().completePrepare(
			sql, stmtId, parameters, columns
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
	protected FieldSetImpl parameters;
	protected FieldSetImpl columns;
	protected int stmtId;
	protected int warnCount;
	protected int lastSeqNum;
	private int paramFieldPos;
	private int colFieldPos;
}
