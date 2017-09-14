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

import com.mysql.cj.api.Session;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import udentric.mysql.MysqlErrorNumbers;
import udentric.mysql.classic.Client;
import udentric.mysql.classic.ColumnDefinition;
import udentric.mysql.classic.Field;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerStatus;
import udentric.mysql.classic.SessionInfo;

public abstract class ResultSet implements Dictum {
	protected ResultSet(
		int columnCount, int lastSeqNum_, ResultSetConsumer rsc_
	) {
		colDef = new ColumnDefinition(columnCount);
		rsc = rsc_;
		lastSeqNum = lastSeqNum_;
	}

	@Override
	public void emitClientMessage(ByteBuf dst, ChannelHandlerContext ctx) {
		throw new UnsupportedOperationException("Not supported.");
	}

	@Override
	public void acceptServerMessage(
		ByteBuf src, ChannelHandlerContext ctx
	) {
		if (Packet.invalidSeqNum(ctx.channel(), src, lastSeqNum + 1))
			return;

		lastSeqNum++;

		src.skipBytes(Packet.HEADER_SIZE);
		SessionInfo si = Client.sessionInfo(ctx.channel());

		if (hasMeta)
			handleRowDataOrEof(src, ctx, si);
		else
			handleColumnMeta(src, ctx, si);
	}

	protected void handleColumnMeta(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		if (!colDef.hasAllFields()) {
			try {
				colDef.appendField(new Field(src, si.charset));
			} catch (Exception e) {
				Client.discardActiveDictum(ctx.channel(), e);
			}

			return;
		}

		if (si.expectEof()) {
			if ((
				Packet.EOF != Packet.readInt1(src)
			) || (src.readableBytes() != 4)) {
				Client.discardActiveDictum(
					ctx.channel(),
					Packet.makeError(
						MysqlErrorNumbers.ER_MALFORMED_PACKET
					)
				);
			} else {
				hasMeta = true;
				rsc.acceptMetadata(colDef);
			}
		} else {
			hasMeta = true;
			rsc.acceptMetadata(colDef);
			handleRowDataOrEof(src, ctx, si);
		}
	}

	private void handleRowDataOrEof(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	) {
		int type = src.getByte(src.readerIndex()) & 0xff;

		switch (type) {
		case Packet.ERR:
			src.skipBytes(1);
			Client.discardActiveDictum(
				ctx.channel(),
				Packet.parseError(src, si.charset)
			);
			return;
		case Packet.EOF:
			if (src.readableBytes() < 0xffffff) {
				src.skipBytes(1);
				Packet.ServerAck ack = new Packet.ServerAck(
					src, !si.expectEof(), si.charset
				);

				if (ServerStatus.MORE_RESULTS_EXISTS.get(
					ack.srvStatus
				)) {
					System.err.format("!!! more results\n");
				}

				Client.discardActiveDictum(ctx.channel());
				rsc.onSuccess(ack);
				return;
			}
		}

		handleRowData(src, ctx, si);
	}

	protected abstract void handleRowData(
		ByteBuf src, ChannelHandlerContext ctx, SessionInfo si
	);

	@Override
	public void handleFailure(Throwable cause) {
		rsc.onFailure(cause);
	}

	protected final ColumnDefinition colDef;
	protected final ResultSetConsumer rsc;
	protected int lastSeqNum;
	protected boolean hasMeta = false;
}
