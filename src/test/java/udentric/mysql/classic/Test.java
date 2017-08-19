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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.concurrent.Future;
import udentric.mysql.classic.auth.NativePasswordCredentialsProvider;

public class Test implements QueryResultConsumer {
	void onConnected(Future<?> f) {
		if (f.isSuccess()) {
			System.out.println("connected!");
			ch.writeAndFlush(Commands.quit()).addListener(
				this::onQuit
			);
			System.out.println("quit sent");
		} else {
			f.cause().printStackTrace();
			ch.close();
		}
	}

	void query() {
		ch.writeAndFlush(
			Commands.query(
				"select * from mysql.user;"
			).withResultConsumer(this)
		);
	}

	void onResult(Future<?> f) {

	}

	void onQuit(Future<?> f) {
		ch.close();
	}

	public static void main(String... args) {
		ResourceLeakDetector.setLevel(Level.PARANOID);

		NioEventLoopGroup grp = new NioEventLoopGroup();
		
		Test t = new Test((new Bootstrap()).group(grp).channel(
			NioSocketChannel.class
		).handler(
			Client.builder().withCredentials(
				new NativePasswordCredentialsProvider()
			).build()
		).connect("10.20.0.10", 3306));
	}

	Test(ChannelFuture chf) {
		ch = chf.channel();
		chf.addListener(this::onConnected);
	}

	Channel ch;
}
