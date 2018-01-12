/*
 * Copyright (c) 2017 - 2018 Alex Dubov <oakad@yahoo.com>
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.BinaryDataRow;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ColumnValueMapper;
import udentric.mysql.classic.FieldSetImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerAck;
import udentric.mysql.classic.SessionInfo;
import udentric.mysql.classic.prepared.Statement;
import udentric.mysql.classic.type.AdapterState;

public class FetchStatementResult implements Dictum {
	public FetchStatementResult(
		PreparedStatement pstmt_, FieldSetImpl columns_,
		ResultSetConsumer rsc_
	) {
		pstmt = (Statement)pstmt_;
		columns = columns_;
		rsc = rsc_;
		lastSeqNum = 0;
		state = this::beginFetch;
	}

	@Override
	public boolean emitClientMessage(
		ByteBuf dst, ChannelHandlerContext ctx
	) {
		fetchCount = rsc.rowFetchCount();
		dst.writeByte(OPCODE);
		dst.writeIntLE(pstmt.getServerId());
		dst.writeIntLE(fetchCount);
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

	@Override
	public void handleFailure(Throwable cause) {
		rsc.acceptFailure(cause);
	}

	private void beginFetch(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		try {
			initRow(rsc.acceptMetadata(columns), ctx.alloc());
		} catch (Exception e) {
			Channels.discardActiveDictum(
				ctx.channel(), e
			);
		}

		state = this::fetchRow;
		fetchRow(src, ctx, si);
	}

	private void fetchRow(
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

				ServerAck ack = si.expectEof()
					? ServerAck.fromEof(src, si)
					: ServerAck.fromOk(src, si);

				Channels.discardActiveDictum(
					ctx.channel()
				);
				rsc.acceptAck(ack, true);
			}
			return;
		}

		src.skipBytes(Packet.HEADER_SIZE);
		if (leftOver == null)
			acceptData(src, ctx, si);
		else
			acceptDataWithLeftOver(src, ctx, si);
	}

	private void acceptData(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		boolean lastBlock = src.readableBytes() < si.packetSize;

		acceptRowData(src);

		if (lastBlock)
			consumeRow(ctx);
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
			consumeRow(ctx);
		} else if (buf.readableBytes() > 0) {
			leftOver = buf.readRetainedSlice(buf.readableBytes());
			buf.release();
		}
	}

	private void initRow(
		ColumnValueMapper mapper_, ByteBufAllocator alloc
	) {
		colState = new AdapterState(alloc);
		mapper = mapper_ != null ? mapper_ : ColumnValueMapper.DEFAULT;
		row = BinaryDataRow.init(row, columns, mapper);
		colDataPos = 0;
		rowConsumed = true;
	}

	private void acceptRowData(ByteBuf src) {
		if (rowConsumed) {
			row.initValues(mapper);
			rowConsumed = false;
			src.skipBytes(1);
			row.readNullBitmap(src);
		}

		for (; colDataPos < columns.size(); colDataPos++) {
			if (row.checkAndSetNull(colDataPos))
				continue;

			row.decodeValue(colDataPos, src, colState, columns);
			if (colState.done())
				colState.reset();
			else
				break;
		}
	}

	private void consumeRow(ChannelHandlerContext ctx) {
		if (colDataPos < columns.size())
			LOGGER.error(
				"Incomplete row read: want {} columns, have {} columns",
				columns.size(), colDataPos
			);

		rsc.acceptRow(row);
		colDataPos = 0;
		rowConsumed = true;
		fetchCount--;

		if (fetchCount == 0) {
			state = this::beginFetch;
			lastSeqNum = 0;
			ctx.channel().writeAndFlush(this).addListener(
				Channels::defaultSendListener
			);
		}
	}

	public static final int OPCODE = 28;

	private static final Logger LOGGER = LogManager.getLogger(
		BinaryResultSet.class
	);

	private final Statement pstmt;
	private final FieldSetImpl columns;
	private final ResultSetConsumer rsc;
	private ServerMessageConsumer state;
	private int fetchCount;
	private int lastSeqNum;
	private ByteBuf leftOver;
	private AdapterState colState;
	private ColumnValueMapper mapper;
	private int colDataPos;
	private boolean rowConsumed;
	private BinaryDataRow row;
}
