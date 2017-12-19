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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import udentric.mysql.ErrorNumbers;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ColumnValueMapper;
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.FieldSetImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerAck;
import udentric.mysql.classic.ServerStatus;
import udentric.mysql.classic.SessionInfo;
import udentric.mysql.classic.type.AdapterState;

public abstract class ResultSet implements Dictum {
	protected ResultSet(
		int columnCount, int lastSeqNum_, ResultSetConsumer rsc_
	) {
		rsc = rsc_;
		lastSeqNum = lastSeqNum_;
		state = this::columnCountReceived;
		columns = new FieldSetImpl(columnCount);
	}

	protected ResultSet(
		int lastSeqNum_, ResultSetConsumer rsc_
	) {
		rsc = rsc_;
		lastSeqNum = lastSeqNum_;
		state = this::beginNextRs;
	}

	@Override
	public boolean emitClientMessage(
		ByteBuf dst, ChannelHandlerContext ctx
	) {
		throw new UnsupportedOperationException("Not supported.");
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

	private void beginNextRs(
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
				ServerAck ack = new ServerAck(
					src, true, si.charset()
				);
				if (ServerStatus.MORE_RESULTS_EXISTS.get(
					ack.srvStatus
				)) {
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
			columns = new FieldSetImpl(
				Packet.readIntLenenc(src)
			);
			colFieldPos = 0;
			state = this::columnCountReceived;
		}
	}

	private void columnCountReceived(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		if (colFieldPos < columns.size()) {
			try {
				src.skipBytes(Packet.HEADER_SIZE);
				columns.set(
					colFieldPos,
					new FieldImpl(src, si.charset())
				);
				colFieldPos++;
			} catch (Exception e) {
				Channels.discardActiveDictum(ctx.channel(), e);
			}
		} else if (si.expectEof()) {
			src.skipBytes(Packet.HEADER_SIZE);
			if ((
				Packet.EOF != Packet.readInt1(src)
			) || (src.readableBytes() != 4)) {
				Channels.discardActiveDictum(
					ctx.channel(),
					Packet.makeError(
						ErrorNumbers.ER_MALFORMED_PACKET
					)
				);
			} else {
				src.skipBytes(4);
				state = this::columnDataReceived;
				initRow(
					rsc.acceptMetadata(columns),
					ctx.alloc()
				);
			}
		} else {
			state = this::columnDataReceived;
			initRow(rsc.acceptMetadata(columns), ctx.alloc());
			columnDataReceived(src, ctx, si);
		}
	}

	private void columnDataReceived(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		int type = src.getByte(
			src.readerIndex() + Packet.HEADER_SIZE
		) & 0xff;

		switch (type) {
		case Packet.ERR:
			src.skipBytes(Packet.HEADER_SIZE + 1);
			Channels.discardActiveDictum(
				ctx.channel(),
				Packet.parseError(src, si.charset())
			);
			return;
		case Packet.EOF:
			if (src.readableBytes() < si.packetSize) {
				src.skipBytes(Packet.HEADER_SIZE + 1);

				ServerAck ack = new ServerAck(
					src, !si.expectEof(), si.charset()
				);
				
				if (ServerStatus.MORE_RESULTS_EXISTS.get(
					ack.srvStatus
				)) {
					state = this::beginNextRs;
					rsc.acceptAck(ack, false);
				} else {
					Channels.discardActiveDictum(
						ctx.channel()
					);
					rsc.acceptAck(ack, true);
				}
				return;
			}
		}

		src.skipBytes(Packet.HEADER_SIZE);
		if (leftOver == null)
			acceptData(src, si);
		else
			acceptDataWithLeftOver(src, ctx, si);
	}

	private void acceptData(ByteBuf src, SessionInfo si) {
		boolean lastBlock = src.readableBytes() < si.packetSize;

		acceptRowData(src);

		if (lastBlock)
			consumeRow();
		else if (src.readableBytes() > 0)
			leftOver = src.readRetainedSlice(src.readableBytes());
	}

	private void acceptDataWithLeftOver(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		boolean lastBlock = src.readableBytes() < si.packetSize;

		src.retain();
		CompositeByteBuf buf = ctx.alloc().compositeBuffer(2);
		buf.addComponents(true, leftOver, src);
		leftOver = buf;

		acceptRowData(buf);

		if (lastBlock) {
			buf.release();
			leftOver = null;
			consumeRow();
		} else if (buf.readableBytes() > 0) {
			leftOver = buf.readRetainedSlice(buf.readableBytes());
			buf.release();
		}
	}

	protected abstract void initRow(
		ColumnValueMapper mapper_, ByteBufAllocator alloc
	);

	protected abstract void acceptRowData(ByteBuf src);

	protected abstract void consumeRow();

	@Override
	public void handleFailure(Throwable cause) {
		if (colState != null)
			colState.reset();

		if (leftOver != null) {
			leftOver.release();
			leftOver = null;
		}

		rsc.acceptFailure(cause);
	}

	protected final ResultSetConsumer rsc;
	protected ServerMessageConsumer state;
	protected FieldSetImpl columns;
	protected int lastSeqNum;
	protected int colFieldPos;
	protected ByteBuf leftOver;
	protected AdapterState colState;
	protected ColumnValueMapper mapper;
	protected int colDataPos;
	protected boolean rowConsumed;
}
