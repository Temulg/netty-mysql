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

import io.netty.channel.ChannelInitializer;
import io.netty.util.AttributeKey;
import io.netty.channel.socket.SocketChannel;
import java.lang.invoke.MethodType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import udentric.mysql.Config;
import udentric.mysql.classic.dicta.Dictum;

public class Client extends ChannelInitializer<SocketChannel> {
	public static class Builder {
		public Builder withConfig(Config config_) {
			config = config_;
			return this;
		}

		public Builder withAttr(String key, String value) {
			connAttributes.put(key, value);
			return this;
		}

		public Client build() {
			return new Client(this);
		}

		private Config config;
		private final LinkedHashMap<
			String, String
		> connAttributes = new LinkedHashMap<>();
	}

	public static Builder builder() {
		return new Builder();
	}

	public Config getConfig() {
		return config;
	}

	private Client(Builder bld) {
		config = bld.config == null
			? Config.fromEnvironment(true).build() : bld.config;

		connAttributes = combineAttributes(bld.connAttributes).build();
	}

	private ImmutableMap.Builder<
		String, String
	> combineAttributes(Map<String, String> extraAttrs) {
		return ImmutableMap.<String, String>builder().put(
			"_os", System.getProperty("os.name")
		).put(
			"_platform", System.getProperty("os.arch")
		).put(
			"_client_name", "netty_mysql"
		).put(
			"_client_version", "1.0"
		).putAll(extraAttrs);
		//_pid
		//program_name
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ch.attr(INITIAL_SESSION_INFO).set(new InitialSessionInfo(this));

		ch.pipeline().addLast(
			"udentric.mysql.classic.InboundPacketFramer",
			new InboundPacketFramer()
		).addLast(
			"udentric.mysql.classic.OutboundMessageHandler",
			OUTBOUND_MESSAGE_HANDLER
		).addLast(
			"udentric.mysql.classic.InboundMessageHandler",
			INBOUND_MESSAGE_HANDLER
		);
	}

	public SocketAddress remoteAddress() {
		System.err.format("-a2-\n");
		String h = config.getOrDefault(Config.Key.HOST, "localhost");
		int port = config.getOrDefault(Config.Key.TCP_PORT, 3306);

		String unix = config.getOrDefault(Config.Key.UNIX_PORT, "");
		SocketAddress rv = null;
		if (h.equals("localhost") && !unix.isEmpty())			
			rv = domainSocketAddress(unix);

		System.err.format("-b2- rv %s\n", rv);
		return rv != null ? rv : InetSocketAddress.createUnresolved(
			h, port
		);
	}

	private static SocketAddress domainSocketAddress(String addr) {
		try {
			Class cls = Client.class.getClassLoader().loadClass(
				"io.netty.channel.unix.DomainSocketAddress"
			);
			MethodHandle h = MethodHandles.lookup().findConstructor(
				cls, MethodType.methodType(
					void.class, String.class
				)
			);
			return (SocketAddress)h.invoke(addr);
		} catch (Throwable e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T extends Throwable> void throwAny(
		Throwable t
	) throws T {
		throw (T)t;
	}

	public static Promise<Packet.ServerAck> newServerPromise(Channel ch) {
		return new DefaultPromise<Packet.ServerAck>(ch.eventLoop());
	}

	public static void discardActiveDictum(Channel ch, Throwable cause) {
		Dictum dct = ch.attr(ACTIVE_DICTUM).getAndSet(null);
		if (dct != null)
			dct.handleFailure(cause);
	}

	public static void discardActiveDictum(Channel ch) {
		ch.attr(ACTIVE_DICTUM).set(null);
	}

	public static SessionInfo sessionInfo(Channel ch) {
		return ch.attr(SESSION_INFO).get();
	}

	public static void defaultSendListener(Future chf) {
		if (!chf.isSuccess())
			discardActiveDictum(
				((ChannelFuture)chf).channel(), chf.cause()
			);
	}

	public static final int MYSQL_PROTOCOL_VERSION = 10;

	public static final AttributeKey<
		SessionInfo
	> SESSION_INFO = AttributeKey.valueOf(
		"udentric.mysql.classic.SessionInfo"
	);
	public static final AttributeKey<
		InitialSessionInfo
	> INITIAL_SESSION_INFO = AttributeKey.valueOf(
		"udentric.mysql.classic.InitialSessionInfo"
	);
	public static final AttributeKey<
		Dictum
	> ACTIVE_DICTUM = AttributeKey.valueOf(
		"udentric.mysql.classic.dicta.Dictum"
	);

	static final OutboundMessageHandler OUTBOUND_MESSAGE_HANDLER
	= new OutboundMessageHandler();
	static final InboundMessageHandler INBOUND_MESSAGE_HANDLER
	= new InboundMessageHandler();

	private final Config config;
	final ImmutableMap<String, String> connAttributes;
}
