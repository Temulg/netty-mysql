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

import java.net.SocketAddress;
import java.util.LinkedHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import udentric.mysql.ServerVersion;
import udentric.mysql.classic.handler.Handshake;
import udentric.mysql.classic.command.Any;
import udentric.mysql.util.ByteString;

public class ProtocolHandler extends ChannelDuplexHandler {
	ProtocolHandler(Client cl_) {
		logger = LogManager.getLogger(ProtocolHandler.class);
		handler = Handshake.INSTANCE;
		cl = cl_;

	}







	public void setChannelSuccess() {
		if (clientPromise != null) {
			ChannelPromise cp = clientPromise;
			clientPromise = null;
			cp.setSuccess();
		}
	}

	public void setChannelFailure(Throwable t) {
		if (clientPromise != null) {
			ChannelPromise cp = clientPromise;
			clientPromise = null;
			cp.setFailure(t);
		} else {
			throwAny(t);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void throwAny(
		Throwable t
	) throws T {
		throw (T)t;
	}

	public ServerVersion getServerVersion() {
		return serverVersion;
	}

	public void updateServerIdentity(
		ByteString srvName, int srvConnId_
	) {
		serverVersion = new ServerVersion(srvName.toString());
		srvConnId = srvConnId_;
		logger.debug(
			"server identity set: {} ({})", srvConnId, srvName
		);
	}

	public void updateServerCharsetId(int charsetId) {
	}

	public void updateServerCapabilities(long caps) {
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
		if (handler == null)
			seqNum = 0;
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

	public Client getClient() {
		return cl;
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
	private ServerVersion serverVersion;
	private long serverCaps;
	private long clientCaps;
	private int srvConnId;
	
}
