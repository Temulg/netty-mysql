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
import io.netty.channel.ChannelPromise;
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

import udentric.mysql.Config;

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
			? Config.fromEnvironment(true) : bld.config;

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
		ch.attr(SESSION).set(new Session(this));
		ch.pipeline().addLast(
			"mysql.packet.in", new InboundPacketFramer()
		).addLast(
			"mysql.command.out", COMMAND_OUT_HANDLER
		).addLast(
			"mysql.response.in", RESPONSE_IN_HANDLER
		);
	}

	public SocketAddress remoteAddress() {
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

	public final static AttributeKey<Session> SESSION = AttributeKey.valueOf(
		"udentric.mysql.classic.Session"
	);

	final static CommandOutHandler COMMAND_OUT_HANDLER
	= new CommandOutHandler();
	final static ResponseInHandler RESPONSE_IN_HANDLER
	= new ResponseInHandler();

	private final Config config;
	final ImmutableMap<String, String> connAttributes;
}
