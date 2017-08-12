/*
 * Copyright (c) 2017 Alex Dubov <oakad@yahoo.com>
 *
 * This file is made available under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package udentric.mysql.classic;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import udentric.mysql.classic.handler.Handshake;

public class ProtocolHandler extends ChannelDuplexHandler {
	ProtocolHandler(Session session_) {
		session = session_;
		handler = new Handshake(session);
	}

	@Override
	public void channelRead(
		ChannelHandlerContext ctx, Object msg
	) throws Exception {
		if (!(msg instanceof Packet)) {
			ctx.fireChannelRead(msg);
			return;
		}
		session.logger.debug("packet in: {}", msg);
		handler = handler.accept(ctx, (Packet)msg);
	}

	private final Session session;
	private MessageHandler handler;
}