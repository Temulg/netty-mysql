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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import udentric.mysql.Config;
import udentric.mysql.classic.dicta.Dictum;

public class Channels {
	private Channels() {
	}

	@SuppressWarnings("unchecked")
	public static <T extends Throwable> void throwAny(
		Throwable t
	) throws T {
		throw (T)t;
	}

	public static ChannelFuture create(
		Config config, Bootstrap bs, Map<String, String> connAttrs
	) {
		InitialSessionInfo si = new InitialSessionInfo(
			config, combineAttributes(connAttrs)
		);

		return bs.handler(new Initializer(si)).connect(
			remoteAddress(config)
		);
	}

	public static ChannelFuture create(
		Config config, Bootstrap bs
	) {
		return create(config, bs, Collections.emptyMap());
	}

	public PoolHandler newPoolHandler(
		Config config, Map<String, String> connAttrs
	) {
		return new PoolHandler(config, combineAttributes(connAttrs));
	}

	public PoolHandler newPoolHandler(Config config) {
		return newPoolHandler(config, Collections.emptyMap());
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

	public static void defaultSendListener(Future<?> chf) {
		if (!chf.isSuccess())
			discardActiveDictum(
				((ChannelFuture)chf).channel(), chf.cause()
			);
	}

	private static Map<String, String> combineAttributes(
		Map<String, String> extraAttrs
	) {
		LinkedHashMap<String, String> m = new LinkedHashMap<>(
			COMMON_CONN_ATTRS
		);
		m.putAll(extraAttrs);
		return m;
	}

	private static SocketAddress remoteAddress(Config config) {
		String h = config.getOrDefault(Config.Key.HOST, "localhost");
		int port = config.getOrDefault(Config.Key.TCP_PORT, 3306);

		String unix = config.getOrDefault(Config.Key.UNIX_PORT, "");
		SocketAddress rv = null;
		if (h.equals("localhost") && !unix.isEmpty())			
			rv = domainSocketAddress(unix);

		return rv != null ? rv : InetSocketAddress.createUnresolved(
			h, port
		);
	}

	private static SocketAddress domainSocketAddress(String addr) {
		try {
			Class<?> cls = Channels.class.getClassLoader().loadClass(
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

	private static void initChannel(Channel ch, InitialSessionInfo si) {
		ch.attr(INITIAL_SESSION_INFO).set(si);

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

	private static class Initializer
	extends ChannelInitializer<SocketChannel> {
		private Initializer(InitialSessionInfo si_) {
			si = si_;
		}

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			Channels.initChannel(ch, si);
		}

		private final InitialSessionInfo si;
	}

	public static class PoolHandler implements ChannelPoolHandler {
		private PoolHandler(
			Config config_, Map<String, String> connAttrs_
		) {
			config = config_;
			connAttrs = connAttrs_;
		}

		@Override
		public void channelReleased(Channel ch) throws Exception {
		}


		@Override
		public void channelAcquired(Channel ch) throws Exception {
		}

		public void channelCreated(Channel ch) throws Exception {
			InitialSessionInfo si = new InitialSessionInfo(
				config, connAttrs
			);
			initChannel(ch, si);
		}

		private final Config config;
		private final Map<String, String> connAttrs;
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

	public static final ImmutableMap<
		String, String
	> COMMON_CONN_ATTRS = ImmutableMap.<String, String>builder().put(
		"_os", System.getProperty("os.name")
	).put(
		"_platform", System.getProperty("os.arch")
	).put(
		"_client_name", "netty_mysql"
	).put(
		"_client_version", "1.0"
	).build();
	//_pid
	//program_name
}
