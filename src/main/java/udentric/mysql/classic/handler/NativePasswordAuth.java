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

package udentric.mysql.classic.handler;

import io.netty.channel.ChannelHandlerContext;
import udentric.mysql.classic.AuthHandler;
import udentric.mysql.classic.MessageHandler;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.Session;

public class NativePasswordAuth extends AuthHandler {
	public NativePasswordAuth(Session session) {
		super(session);
	}

	@Override
	public void setInitialSecret(byte[] secret_) {
		secret = secret_;
	}

	@Override
	public MessageHandler activate(
		ChannelHandlerContext ctx, int nextSeqNum
	) {
		Packet out = new Packet();
		out.seqNum = nextSeqNum;
		out.body = ctx.alloc().buffer();

		out.body.writeIntLE(
			(int)(session.getClientCapabilities() & 0xffffffff)
		);
		out.body.writeIntLE(0xffffff); // 16mb
		out.body.writeByte(224); //utf8mb4
		out.body.writeZero(23);
		// write username
		
		return this;
	}

	@Override
	public MessageHandler accept(ChannelHandlerContext ctx, Packet p) {
		return null;
	}

	private byte[] secret;
}
