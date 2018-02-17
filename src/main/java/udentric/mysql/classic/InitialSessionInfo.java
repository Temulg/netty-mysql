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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.Promise;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import udentric.mysql.Config;
import udentric.mysql.Encoding;
import udentric.mysql.ErrorNumbers;
import udentric.mysql.ServerVersion;
import udentric.mysql.classic.dicta.Dictum;
import udentric.mysql.classic.dicta.InitDb;
import udentric.mysql.classic.dicta.MysqlNativePasswordAuth;


public class InitialSessionInfo {
	InitialSessionInfo(
		Config config_, Map<String, String> connAttributes_
	) {
		config = config_;
		connAttributes = connAttributes_;
		encoding = selectEncoding();
		packetSize = config.getOrDefault(
			Config.Key.maxPacketSize, 0xffffff
		);
	}

	public Charset charset() {
		return encoding.charset;
	}

	private Encoding selectEncoding() {
		String csetName = config.getOrDefault(
			Config.Key.characterEncoding, null
		);

		if (csetName == null)
			return Encoding.forId(223);

		// handle collation

		Map<String, Encoding> colMap = Encoding.forCharset(csetName);

		for (Encoding enc: colMap.values()) {
			preferLocalEncoding = true;
			return enc;
		}

		return Encoding.forId(223);
	}

	public void onAuth(
		ByteBuf msg, ChannelHandlerContext ctx, ChannelPromise chp
	) {
		SessionInfo si = new SessionInfo(this);
		ServerAck ack = ServerAck.fromOk(msg, si);

		LOGGER.debug(() -> {
			return new ParameterizedMessage(
				"authenticated: {}", ack.info
			);
		});

		Channel ch = ctx.channel();
		Channels.discardActiveDictum(ch);

		ch.attr(Channels.SESSION_INFO).set(si);
		ch.attr(Channels.INITIAL_SESSION_INFO).set(null);

		String schema = config.getOrDefault(
			Config.Key.DBNAME, ""
		);

		if (schema.isEmpty() || ClientCapability.CONNECT_WITH_DB.get(
			clientCaps
		)) {
			chp.setSuccess();
			return;
		}

		Promise<
			udentric.mysql.ServerAck
		> sp = Channels.newServerPromise(ch);

		sp.addListener(sf -> {
			if (sf.isSuccess())
				chp.setSuccess();
			else
				chp.setFailure(sf.cause());
		});

		ch.writeAndFlush(new InitDb(schema, sp)).addListener(
			Channels::defaultSendListener
		);
	}

	public void processInitialHandshake(
		ByteBuf src, ChannelHandlerContext ctx, ChannelPromise chp
	) {
		Channel ch = ctx.channel();
		try {
			Channels.discardActiveDictum(ch);
			ch.writeAndFlush(
				decodeInitialHandshake(src, ctx, chp)
			).addListener(Channels::defaultSendListener);
		} catch (Exception e) {
			chp.setFailure(e);
		}
	}

	private Dictum decodeInitialHandshake(
		ByteBuf src, ChannelHandlerContext ctx, ChannelPromise chp
	) {
		int seqNum = Packet.getSeqNum(src);
		src.skipBytes(Packet.HEADER_SIZE);

		int protoVers = Packet.readInt1(src);
		if (Channels.MYSQL_PROTOCOL_VERSION != protoVers) {
			throw new DecoderException(
				"unsupported protocol version "
				+ Integer.toString(protoVers)
			);
		}

		String authPluginName
		= MysqlNativePasswordAuth.AUTH_PLUGIN_NAME;

		version = new ServerVersion(Packet.readStringNT(
			src, StandardCharsets.UTF_8
		));
		srvConnId = src.readIntLE();
		LOGGER.debug(() -> {
			return new ParameterizedMessage(
				"server identity set: {} ({})",
				srvConnId, version
			);
		});

		secret = new byte[8];
		src.readBytes(secret);
		src.skipBytes(1);

		if (!src.isReadable()) {
			return selectAuthCommand(
				authPluginName, seqNum, ctx, chp
			);
		}

		serverCaps = Packet.readLong2(src);

		if (!src.isReadable()) {
			updateClientCapabilities();
			return selectAuthCommand(
				authPluginName, seqNum, ctx, chp
			);
		}

		Encoding serverEncoding = Encoding.forId(Packet.readInt1(src));

		if (!preferLocalEncoding)
			encoding = serverEncoding;

		src.skipBytes(2);//short statusFlags = msg.readShortLE();

		serverCaps |= Packet.readLong2(src) << 16;
		updateClientCapabilities();
		int s2Len = Packet.readInt1(src);
		src.skipBytes(10);

		if (ClientCapability.PLUGIN_AUTH.get(serverCaps)) {
			int oldLen = secret.length;
			secret = Arrays.copyOf(secret, s2Len - 1);
			src.readBytes(
				secret, oldLen, s2Len - oldLen - 1
			);
			src.skipBytes(1);
			authPluginName = Packet.readStringNT(
				src, serverEncoding.charset
			);
		} else {
			s2Len = Math.max(12, src.readableBytes());
			int oldLen = secret.length;
			secret = Arrays.copyOf(secret, oldLen + s2Len);
			src.readBytes(secret, oldLen, s2Len);
		}

		if (ClientCapability.CONNECT_ATTRS.get(clientCaps)) {
			attrBuf = encodeAttrs(ctx);
		}

		if (ClientCapability.CONNECT_WITH_DB.get(clientCaps)) {
			schema = config.getOrDefault(
				Config.Key.DBNAME, ""
			);
			if (schema.isEmpty())
				clientCaps &= ~ClientCapability.CONNECT_WITH_DB.mask();
		}

		return selectAuthCommand(
			authPluginName, seqNum, ctx, chp
		);
	}

	private Dictum selectAuthCommand(
		String authPluginName, int seqNum,
		ChannelHandlerContext ctx, ChannelPromise chp
	)  {
		switch (authPluginName) {
		case "mysql_native_password":
			++seqNum;

			return new MysqlNativePasswordAuth(
				this, seqNum, chp
			);
		default:
			throw Packet.makeError(
				ErrorNumbers.ER_NOT_SUPPORTED_AUTH_MODE,
				authPluginName
			);
		}
	}

	private ByteBuf encodeAttrs(ChannelHandlerContext ctx) {
		ByteBuf buf = ctx.alloc().buffer();
		connAttributes.forEach((k, v) -> {
			byte[] b = k.getBytes(encoding.charset);
			Packet.writeIntLenenc(buf, b.length);
			buf.writeBytes(b);
			b = v.getBytes(encoding.charset);
			Packet.writeIntLenenc(buf, b.length);
			buf.writeBytes(b);
		});
		return buf;
	}

	private void updateClientCapabilities() {
		clientCaps = ClientCapability.LONG_PASSWORD.mask()
			| ClientCapability.LONG_FLAG.mask()
			| ClientCapability.CONNECT_WITH_DB.mask()
			| ClientCapability.LOCAL_FILES.mask()
			| ClientCapability.PROTOCOL_41.mask()
			| ClientCapability.TRANSACTIONS.mask()
			| ClientCapability.MULTI_STATEMENTS.mask()
			| ClientCapability.MULTI_RESULTS.mask()
			| ClientCapability.PS_MULTI_RESULTS.mask()
			| ClientCapability.PLUGIN_AUTH.mask()
			| ClientCapability.CONNECT_ATTRS.mask()
			| ClientCapability.DEPRECATE_EOF.mask();

		clientCaps &= serverCaps;
	}

	public static final Logger LOGGER = LogManager.getLogger(
		InitialSessionInfo.class
	);

	public final Config config;
	private final Map<String, String> connAttributes;
	private boolean preferLocalEncoding = false;
	public ServerVersion version;
	public Encoding encoding;
	public long serverCaps;
	public long clientCaps;
	public int srvConnId;
	public int packetSize;
	public byte[] secret;
	public ByteBuf attrBuf;
	public String schema = "";
}
