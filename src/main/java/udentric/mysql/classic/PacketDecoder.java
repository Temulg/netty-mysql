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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ByteToMessageDecoder.Cumulator;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.classic.message.Packet;

public class PacketDecoder extends ChannelInboundHandlerAdapter {
	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		ByteBuf acc = accumulator;
		if (acc != null) {
			accumulator = null;
			int available = acc.readableBytes();
			if (available > 0) {
				ByteBuf out = acc.readBytes(available);
				acc.release();
				ctx.fireChannelRead(out);
				ctx.fireChannelReadComplete();
			} else
				acc.release();
		}
	}

	@Override
	public void channelRead(
		ChannelHandlerContext ctx, Object msg_
	) throws Exception {
		if (!(msg_ instanceof ByteBuf)) {
			ctx.fireChannelRead(msg_);
			return;
		}

		try {
			ByteBuf msg = (ByteBuf)msg_;
			if (accumulator == null) {
				accumulator = msg;
			} else {
				accumulator = cumulator.cumulate(
					ctx.alloc(), accumulator, msg
				);
			}

			extractPackets(ctx, accumulator);
		} catch (Throwable e) {
			throw new DecoderException(e);
		} finally {
			if (accumulator != null && !accumulator.isReadable()) {
				accumulator.release();
				accumulator = null;
			}
		}
	}

	protected boolean extractPackets(ChannelHandlerContext ctx, ByteBuf in) {
		boolean rv = false;

		while (in.isReadable()) {
			int available = in.readableBytes();
			if (available < 4)
				break;

			int header = in.getIntLE(in.readerIndex());
			int pSize = header & 0xffffff;
			if (available < (pSize + 4))
				break;

			Packet out = new Packet();
			out.length = pSize;
			out.seqNum = header >> 24;
			in.skipBytes(4);
			out.body = in.retainedSlice(in.readerIndex(), pSize);
			in.skipBytes(pSize);
			ctx.fireChannelRead(out);
			rv = true;
			
			if (ctx.isRemoved())
				break;
		}
		return rv;
	}

	@Override
	public void userEventTriggered(
		ChannelHandlerContext ctx, Object evt
	) throws Exception {
		if (evt instanceof ChannelInputShutdownEvent)
			channelInputClosed(ctx);

		super.userEventTriggered(ctx, evt);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		try {
			channelInputClosed(ctx);
		} finally {
			ctx.fireChannelInactive();
		}
	}

	private void channelInputClosed(
		ChannelHandlerContext ctx
	) throws Exception {
		if (accumulator == null)
			return;

		boolean anyReads = false;
		try {
			anyReads = extractPackets(ctx, accumulator);
		} catch (Throwable e) {
			throw new DecoderException(e);
		} finally {
			if (accumulator != null) {
				accumulator.release();
				accumulator = null;
			}
			if (anyReads)
				ctx.fireChannelReadComplete();
		}		
	}

	ByteBuf accumulator;
	private Cumulator cumulator = ByteToMessageDecoder.MERGE_CUMULATOR;
}
