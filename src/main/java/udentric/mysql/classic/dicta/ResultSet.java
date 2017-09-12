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
import udentric.mysql.classic.Fields;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResponseConsumer;
import udentric.mysql.classic.ResponseType;
import udentric.mysql.classic.Session;
import udentric.mysql.exceptions.MysqlErrorNumbers;

public abstract class ResultSet implements Dictum {
	protected ResultSet(
		int columnCount, boolean expectEof_, int lastSeqNum_,
		CharsetInfo.Entry charset_
	) {
		columns = new ColumnDefinition[columnCount];
		expectEof = expectEof_;
		lastSeqNum = lastSeqNum_;
		charset = charset_;
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
			handleRowData(src, ss, ctx);
		else
			handleColumnMeta(src, ss, ctx);
	}

	protected void handleColumnMeta(
		ByteBuf src, Session ss, ChannelHandlerContext ctx
	) {
		if (receivedColumns >= columns.length) {
			if (expectEof) {
				if ((
					ResponseType.EOF != Fields.readInt1(src)
				) || (src.readableBytes() != 4)) {
					ss.discardCommand(Packet.makeError(
						MysqlErrorNumbers.ER_MALFORMED_PACKET
					));
				} else
					hasMeta = true;
			} else {
				hasMeta = true;
				handleRowData(src, ss, ctx);
			}
			return;
		}

		try {
			columns[receivedColumns] = new ColumnDefinition(
				src, charset
			);
		} catch (Exception e) {
			ss.discardCommand(e);
		}
		receivedColumns++;
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

	protected final ColumnDefinition[] columns;
	protected final boolean expectEof;
	protected final CharsetInfo.Entry charset;
	protected ResponseConsumer rc;
	protected ChannelPromise chp;
	protected int lastSeqNum;
	protected boolean hasMeta = false;
	protected int receivedColumns = 0;
}
