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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.concurrent.Future;

public class Test {
	static class InitHandler extends ChannelInitializer<SocketChannel> {
		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			ch.pipeline().addLast(
				"mysql.packet.in", new PacketDecoder()
			).addLast(
				"mysql.protocol", new Protocol()
			).addLast(new Session());
		}
	}

	static class Session extends SimpleChannelInboundHandler<Object> {
		@Override
		protected void channelRead0(
			ChannelHandlerContext ctx, Object msg
		) throws Exception {
			System.out.format(
				"msg in %s, class %s\n", msg, msg.getClass()
			);
			
		}		
	}

	static void onConnected(Future<?> f) {
		if (f.isSuccess()) {
			System.out.println("connected!");
		} else {
			f.cause().printStackTrace();
		}
	}

	public static void main(String... args) {
		ResourceLeakDetector.setLevel(Level.PARANOID);

		NioEventLoopGroup grp = new NioEventLoopGroup();
		
		(new Bootstrap()).group(grp).channel(
			NioSocketChannel.class
		).handler(
			new InitHandler()
		).connect("10.112.0.132", 3306).addListener(Test::onConnected);
	}
}
