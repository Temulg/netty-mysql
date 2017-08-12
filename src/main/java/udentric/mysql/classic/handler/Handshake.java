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
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.classic.Fields;
import udentric.mysql.classic.MessageHandler;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.AuthHandler;
import udentric.mysql.classic.ClientCapability;
import udentric.mysql.classic.ServerStatus;
import udentric.mysql.classic.Session;

public class Handshake extends MessageHandler {
	public Handshake(Session session) {
		super(session);
	}

	@Override
	public MessageHandler activate(
		ChannelHandlerContext ctx, int nextSeqNum
	) {
		return this;
	}

	@Override
	public MessageHandler accept(ChannelHandlerContext ctx, Packet p) {
		ByteBuf in = p.body;

		int protoVers = Fields.readInt1(in);
		if (PROTOCOL_VERSION != protoVers) {
			in.release();
			throw new DecoderException(
				"unsupported protocol version "
				+ Integer.toString(protoVers)
			);
		}

		String authPluginName = "mysql_native_password";

		session.setServerIdentity(
			Fields.readStringNT(in), in.readIntLE()
		);

		byte[] scramble = Fields.readBytes(in, 8);
		in.skipBytes(1);

		long srvCaps = 0;

		if (!in.isReadable()) {
			checkAndReleaseBuffer(in);
			return advance(
				ctx, authPluginName, scramble, p.seqNum + 1
			);
		}

		srvCaps = Fields.readLong2(in);

		if (!in.isReadable()) {
			checkAndReleaseBuffer(in);
			return advance(
				ctx, authPluginName, scramble, p.seqNum + 1
			);
		}

		session.setServerCharsetId(Fields.readInt1(in));

		short statusFlags = in.readShortLE();

		session.logger.debug(ServerStatus.describe(statusFlags));

		srvCaps |= Fields.readLong2(in) << 16;

		int s2Len = Fields.readInt1(in);

		session.setServerCapabilities(srvCaps);

		in.skipBytes(10);

		if (ClientCapability.PLUGIN_AUTH.get(srvCaps)) {
			int oldLen = scramble.length;
			scramble = Arrays.copyOf(scramble, s2Len - 1);
			in.readBytes(scramble, oldLen, s2Len - oldLen - 1);
			in.skipBytes(1);
			authPluginName = Fields.readStringNT(in).toString();
		} else {
			s2Len = Math.max(12, in.readableBytes());
			int oldLen = scramble.length;
			scramble = Arrays.copyOf(scramble, oldLen + s2Len);
			in.readBytes(scramble, oldLen, s2Len);
		}

		session.logger.debug("plugin name {}", authPluginName);
		session.logger.debug("scramble {}", Arrays.toString(scramble));
		checkAndReleaseBuffer(in);
		return advance(
			ctx, authPluginName, scramble, p.seqNum + 1
		);
	}

	private void checkAndReleaseBuffer(ByteBuf in) {
		int remaining = in.readableBytes();

		if (remaining > 0) {
			session.logger.warn(
				"{} bytes left in packet", remaining
			);
		}
		in.release();
	}

	private MessageHandler advance(
		ChannelHandlerContext ctx, String authPluginName,
		byte[] scramble, int nextSeqNum
	) {
		AuthHandler ah = AUTH_PLUGINS.getOrDefault(
			authPluginName, s -> {
				return null;
		}).apply(session);

		if (ah == null) {
			throw new DecoderException(
				"unsupported authentication plugin "
				+ authPluginName
			);
		}

		ah.setInitialSecret(scramble);

		return ah.activate(ctx, nextSeqNum);
	}

	public static final int PROTOCOL_VERSION = 10;

	public static final ImmutableMap<
		String, Function<Session, AuthHandler>
	> AUTH_PLUGINS = ImmutableMap.<
		String, Function<Session, AuthHandler>
	>builder().put(
		"mysql_native_password", NativePasswordAuth::new
	).build();
}
