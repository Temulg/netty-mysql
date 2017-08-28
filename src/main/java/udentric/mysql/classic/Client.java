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
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import udentric.mysql.Config;
import udentric.mysql.classic.auth.CredentialsProvider;

public class Client extends ChannelInitializer<SocketChannel> {
	public static class Builder {
		public Builder withConfig(Config config_) {
			config = config_;
			return this;
		}

		public Builder withCredentials(CredentialsProvider creds_) {
			creds = creds_;
			return this;
		}

		public Client build() {
			return new Client(this);
		}

		private Config config;
		private CredentialsProvider creds;
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
		creds = bld.creds;
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ch.pipeline().addLast(
			"mysql.packet.in", new InboundPacketFramer()
		).addLast(
			"mysql.protocol", new ProtocolHandler(this)
		);
	}

	public SocketAddress remoteAddress() {
		String h = config.getOrDefault(Config.Key.HOST, "localhost");
		int port = config.getOrDefault(Config.Key.TCP_PORT, 3306);
		/*
		String unix = config.getOrDefault(Config.Key.UNIX_PORT, "");
		if (h.equals("localhost") && !unix.isEmpty()) {
			return DomainSocketAddress(unix);
		}
		*/
		return InetSocketAddress.createUnresolved(h, port);
	}

	private final Config config;
	final CredentialsProvider creds;
}
