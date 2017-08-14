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

import java.util.LinkedHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import udentric.mysql.classic.handler.Handshake;
import udentric.mysql.util.ByteString;

public class ProtocolHandler extends ChannelDuplexHandler {
	ProtocolHandler(Client cl_) {
		logger = LogManager.getLogger(ProtocolHandler.class);
		handler = Handshake.INSTANCE;
		cl = cl_;
		addCommonAttributes();
	}

	private void addCommonAttributes() {
		connAttributes.put(
			new ByteString("_os"),
			new ByteString(System.getProperty("os.name"))
		);
		connAttributes.put(
			new ByteString("_platform"),
			new ByteString(System.getProperty("os.arch"))
		);
		connAttributes.put(
			new ByteString("_client_name"),
			new ByteString("netty_mysql")
		);
		connAttributes.put(
			new ByteString("_client_version"),
			new ByteString("1.0")
		);
		//_pid
		//program_name
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
		ByteString srvName_, int srvConnId_
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

		clientCaps = ClientCapability.LONG_PASSWORD.mask()
			| ClientCapability.LONG_FLAG.mask()
			| ClientCapability.PROTOCOL_41.mask()
			| ClientCapability.TRANSACTIONS.mask()
			| ClientCapability.MULTI_STATEMENTS.mask()
			| ClientCapability.MULTI_RESULTS.mask()
			| ClientCapability.PS_MULTI_RESULTS.mask()
			| ClientCapability.PLUGIN_AUTH.mask();

		if (ClientCapability.CONNECT_ATTRS.get(serverCaps))
			clientCaps |= ClientCapability.CONNECT_ATTRS.mask();

		if (cl.creds.isSecure()) {
			clientCaps |= ClientCapability.SECURE_CONNECTION.mask();
			if (ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(serverCaps))
				clientCaps |= ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.mask();
		}
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

	public void updateAuthReply(
		ByteString authPluginName, byte[] secret
	) {
		cl.creds.updateAuthMessage(
			this, replyMsg, authPluginName, secret
		);
	}

	public void setMessageHandler(MessageHandler handler_) {
		handler = handler_;
	}

	public void sendReply() {
		ByteBuf msg = replyMsg;
		replyMsg = null;

		if (ClientCapability.CONNECT_ATTRS.get(clientCaps)) {
			ByteBuf attrs = encodeAttrs();
			Fields.writeIntLenenc(msg, attrs.readableBytes());
			msg.writeBytes(attrs);
			attrs.release();
		}

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

	private ByteBuf encodeAttrs() {
		ByteBuf rv = channelCtx.alloc().buffer();
		connAttributes.forEach((k, v) -> {
			Fields.writeIntLenenc(rv, k.size());
			rv.writeBytes(k.getBytes());
			Fields.writeIntLenenc(rv, v.size());
			rv.writeBytes(v.getBytes());
		});
		return rv;
	}

	public final Logger logger;
	private final Client cl;
	private final LinkedHashMap<ByteString, ByteString> connAttributes
	= new LinkedHashMap<>();
	private ChannelHandlerContext channelCtx;
	private MessageHandler handler;
	private ByteBuf replyMsg;
	private String srvName;
	private long serverCaps;
	private long clientCaps;
	private int srvConnId;
	private int seqNum;
}
