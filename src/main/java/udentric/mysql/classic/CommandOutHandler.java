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

package udentric.mysql.classic;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;
import udentric.mysql.classic.command.Any;
import udentric.mysql.classic.command.Handshake;

@Sharable
class CommandOutHandler extends ChannelOutboundHandlerAdapter {
	@Override
	public void connect(
		ChannelHandlerContext ctx, SocketAddress remoteAddress,
		SocketAddress localAddress, ChannelPromise promise
	) throws Exception {
		Channel ch = ctx.channel();
		Session cs = ch.attr(Client.SESSION).get();

		{
			Throwable t = cs.beginRequest(new Handshake(promise));
			if (t != null) {
				promise.setFailure(t);
				return;
			}
		}

		ChannelPromise nextPromise = ch.newPromise();
		nextPromise.addListener(chf -> {
			if (!chf.isSuccess()) {
				cs.processClientFailure(chf.cause());
			}
		});

		ctx.connect(remoteAddress, localAddress, nextPromise);
	}

	@Override
	public void write(
		ChannelHandlerContext ctx, Object msg_, ChannelPromise promise
	) throws Exception {
		if (!(msg_ instanceof Any)) {
			super.write(ctx, msg_, promise);
			return;
		}

		Any msg = (Any)msg_;
		Session cs = ctx.channel().attr(Client.SESSION).get();

		{
			Throwable t = cs.beginRequest(msg);
			if (t != null) {
				promise.setFailure(t);
				return;
			}
		}

		try {
			ByteBuf dst = ctx.alloc().buffer();
			int wpos = dst.writerIndex();
			dst.writeZero(Packet.HEADER_SIZE);
			msg.encode(dst, cs);
			int len = dst.writerIndex() - wpos - Packet.HEADER_SIZE;
			dst.setMediumLE(wpos, len);
			dst.setByte(wpos + 3, cs.getSeqNum());
			super.write(ctx, dst, promise);
		} catch (Throwable t) {
			promise.setFailure(t);
		}
	}
}
