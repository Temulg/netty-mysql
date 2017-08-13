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

import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.classic.Fields;
import udentric.mysql.classic.MessageHandler;
import udentric.mysql.classic.ProtocolHandler;
import udentric.mysql.classic.ClientCapability;
import udentric.mysql.classic.ServerStatus;

public class Handshake implements MessageHandler {
	private Handshake() {}

	@Override
	public void process(ProtocolHandler ph, ByteBuf msg) {
		int protoVers = Fields.readInt1(msg);
		if (PROTOCOL_VERSION != protoVers) {
			throw new DecoderException(
				"unsupported protocol version "
				+ Integer.toString(protoVers)
			);
		}

		String authPluginName = "mysql_native_password";

		ph.updateServerIdentity(
			Fields.readStringNT(msg), msg.readIntLE()
		);

		byte[] scramble = Fields.readBytes(msg, 8);
		msg.skipBytes(1);

		long srvCaps = 0;

		if (!msg.isReadable()) {
			reply(ph, authPluginName, scramble);
			return;
		}

		srvCaps = Fields.readLong2(msg);

		if (!msg.isReadable()) {
			ph.updateServerCapabilities(srvCaps);
			reply(ph, authPluginName, scramble);
			return;
		}

		ph.updateServerCharsetId(Fields.readInt1(msg));

		short statusFlags = msg.readShortLE();

		ph.logger.debug(ServerStatus.describe(statusFlags));

		srvCaps |= Fields.readLong2(msg) << 16;

		int s2Len = Fields.readInt1(msg);

		ph.updateServerCapabilities(srvCaps);

		msg.skipBytes(10);

		if (ClientCapability.PLUGIN_AUTH.get(srvCaps)) {
			int oldLen = scramble.length;
			scramble = Arrays.copyOf(scramble, s2Len - 1);
			msg.readBytes(scramble, oldLen, s2Len - oldLen - 1);
			msg.skipBytes(1);
			authPluginName = Fields.readStringNT(msg).toString();
		} else {
			s2Len = Math.max(12, msg.readableBytes());
			int oldLen = scramble.length;
			scramble = Arrays.copyOf(scramble, oldLen + s2Len);
			msg.readBytes(scramble, oldLen, s2Len);
		}

		ph.logger.debug("plugin name {}", authPluginName);
		ph.logger.debug("scramble {}", Arrays.toString(scramble));

		reply(ph, authPluginName, scramble);
	}

	private void reply(
		ProtocolHandler ph, String authPluginName, byte[] scramble
	) {
		ByteBuf replyMsg = ph.allocReplyMsg();
		replyMsg.writeIntLE(
			(int)(ph.getClientCapabilities() & 0xffffffff)
		);
		replyMsg.writeIntLE(0xffffff); // 24MB
		replyMsg.writeByte(224); // utf8mb4
		replyMsg.writeZero(23);
		ph.updateAuthReply(authPluginName, scramble);
		ph.sendReply();
	}

	public static final Handshake INSTANCE = new Handshake();

	public final int PROTOCOL_VERSION = 10;
}
