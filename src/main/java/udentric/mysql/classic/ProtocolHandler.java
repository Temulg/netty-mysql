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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import udentric.mysql.classic.handler.Handshake;

public class ProtocolHandler extends ChannelDuplexHandler {
	ProtocolHandler(Client cl_) {
		logger = LogManager.getLogger(ProtocolHandler.class);
		handler = Handshake.INSTANCE;
		cl = cl_;
	}

	@Override
	public void channelRead(
		ChannelHandlerContext ctx, Object msg_
	) throws Exception {
		if (!(msg_ instanceof ByteBuf)) {
			ctx.fireChannelRead(msg_);
			return;
		}

		channelCtx = ctx;
		ByteBuf msg = (ByteBuf)msg_;
		seqNum = Packet.getSeqNum(msg);
		msg.skipBytes(Packet.HEADER_SIZE);

		try {
			handler.process(this, msg);
		} finally {
			channelCtx = null;
			int remaining = msg.readableBytes();
			if (remaining > 0) {
				logger.warn(
					"{} bytes left in incoming packet",
					remaining
				);
			}
			msg.release();

			if (replyMsg != null) {
				replyMsg.release();
				replyMsg = null;
			}
		}
	}

	public void updateServerIdentity(
		CharSequence srvName_, int srvConnId_
	) {
		srvName = srvName_.toString();
		srvConnId = srvConnId_;
		logger.debug(
			"server identity set: {} ({})", srvConnId, srvName
		);
	}

	public void updateServerCharsetId(int charsetId) {
	}

	public void updateServerCapabilities(long caps) {
		logger.debug(ClientCapability.describe(caps));
		serverCaps = caps;

		clientCaps = ClientCapability.PLUGIN_AUTH.mask()
			| ClientCapability.LONG_PASSWORD.mask()
			| ClientCapability.PROTOCOL_41.mask()
			| ClientCapability.TRANSACTIONS.mask()
			| ClientCapability.MULTI_RESULTS.mask()
			| ClientCapability.SECURE_CONNECTION.mask()
			| ClientCapability.MULTI_STATEMENTS.mask();

		if (ClientCapability.CAN_HANDLE_EXPIRED_PASSWORDS.get(serverCaps))
			clientCaps |= ClientCapability.CAN_HANDLE_EXPIRED_PASSWORDS.mask();

		if (ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(serverCaps))
			clientCaps |= ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.mask();
	}

	public long getClientCapabilities() {
		return clientCaps;
	}

	public long getServerCapabilities() {
		return serverCaps;
	}

	public ByteBuf allocReplyMsg() {
		replyMsg = channelCtx.alloc().buffer();
		replyMsg.writeZero(Packet.HEADER_SIZE);
		return replyMsg;
	}

	public void updateAuthReply(String authPluginName, byte[] scramble) {
		cl.creds.updateAuthMessage(
			this, replyMsg, authPluginName, scramble
		);
	}

	public void setMessageHandler(MessageHandler handler_) {
		handler = handler_;
	}

	public void sendReply() {
		ByteBuf msg = replyMsg;
		replyMsg = null;

		int wPos = msg.writerIndex();
		msg.setMediumLE(0, wPos - Packet.HEADER_SIZE);
		seqNum++;
		msg.setByte(3, seqNum);
		channelCtx.writeAndFlush(msg).addListener(f -> {
			if (!f.isSuccess()) {
				logger.error(
					"exception sending message", f.cause()
				);
			}
		});
	}

	public final Logger logger;
	private final Client cl;
	private ChannelHandlerContext channelCtx;
	private MessageHandler handler;
	private ByteBuf replyMsg;
	private String srvName;
	private long serverCaps;
	private long clientCaps;
	private int srvConnId;
	private int seqNum;
}
