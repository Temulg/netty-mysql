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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import udentric.mysql.classic.CharsetInfo;
import udentric.mysql.classic.ColumnDefinition;
import udentric.mysql.classic.Field;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResponseConsumer;
import udentric.mysql.classic.Session;
import udentric.mysql.exceptions.MysqlErrorNumbers;

public abstract class ResultSet implements Dictum {
	protected ResultSet(
		int columnCount, boolean expectEof_, int lastSeqNum_,
		CharsetInfo.Entry charset_
	) {
		expectEof = expectEof_;
		lastSeqNum = lastSeqNum_;
		charset = charset_;
		colDef = new ColumnDefinition(columnCount);
	}

	public ResultSet withResponseConsumer(ResponseConsumer rc_) {
		rc = rc_;
		return this;
	}

	@Override
	public ChannelPromise channelPromise() {
		return chp;
	}

	@Override
	public ResultSet withChannelPromise(ChannelPromise chp_) {
		chp = chp_;
		return this;
	}

	@Override
	public void encode(ByteBuf dst, Session ss) {
		throw new UnsupportedOperationException("Not supported.");
	}

	@Override
	public void handleReply(ByteBuf src, Session ss, ChannelHandlerContext ctx) {
		int nextSeqNum = Packet.getSeqNum(src);
		if ((lastSeqNum + 1) != nextSeqNum) {
			ss.discardCommand(Packet.makeError(
				MysqlErrorNumbers.ER_NET_PACKETS_OUT_OF_ORDER
			));
			return;
		} else
			lastSeqNum = nextSeqNum;

		src.skipBytes(Packet.HEADER_SIZE);

		if (hasMeta)
			handleRowDataOrEof(src, ss, ctx);
		else
			handleColumnMeta(src, ss, ctx);
	}

	protected void handleColumnMeta(
		ByteBuf src, Session ss, ChannelHandlerContext ctx
	) {
		if (colDef.hasAllFields()) {
			if (expectEof) {
				if ((
					Packet.EOF != Packet.readInt1(src)
				) || (src.readableBytes() != 4)) {
					ss.discardCommand(Packet.makeError(
						MysqlErrorNumbers.ER_MALFORMED_PACKET
					));
				} else
					hasMeta = true;
			} else {
				hasMeta = true;
				handleRowDataOrEof(src, ss, ctx);
			}

			if (hasMeta) {
				if (rc != null)
					rc.onMetadata(colDef);
			}
			return;
		}

		try {
			colDef.appendField(new Field(src, charset));
		} catch (Exception e) {
			ss.discardCommand(e);
		}

	}

	private void handleRowDataOrEof(
		ByteBuf src, Session ss, ChannelHandlerContext ctx
	) {
		int type = src.getByte(src.readerIndex()) & 0xff;

		switch (type) {
		case Packet.ERR:
			src.skipBytes(1);
			ss.discardCommand(Packet.parseError(src, charset));
			return;
		case Packet.EOF:
			if (src.readableBytes() < 0xffffff) {
				src.skipBytes(1);
				Packet.ServerAck ack = new Packet.ServerAck(
					src, !expectEof, charset
				);
				ss.discardCommand();
				if (rc != null)
					rc.onSuccess(ack);
				if (chp != null)
					chp.setSuccess();
				return;
			}
		}

		handleRowData(src, ss, ctx);
	}

	protected abstract void handleRowData(
		ByteBuf src, Session ss, ChannelHandlerContext ctx
	);

	@Override
	public void handleFailure(Throwable cause) {
		if (rc != null)
			rc.onFailure(cause);
		if (chp != null)
			chp.setFailure(cause);
	}

	protected final boolean expectEof;
	protected final CharsetInfo.Entry charset;
	protected final ColumnDefinition colDef;
	protected ResponseConsumer rc;
	protected ChannelPromise chp;
	protected int lastSeqNum;
	protected boolean hasMeta = false;
}
