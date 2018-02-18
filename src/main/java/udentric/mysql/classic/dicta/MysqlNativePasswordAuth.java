/*
 * Copyright (c) 2017 - 2018 Alex Dubov <oakad@yahoo.com>
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

package udentric.mysql.classic.dicta;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.Config;
import udentric.mysql.ErrorNumbers;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ClientCapability;
import udentric.mysql.classic.InitialSessionInfo;
import udentric.mysql.classic.Packet;
import udentric.mysql.util.Scramble411;

public class MysqlNativePasswordAuth implements Dictum {
	public MysqlNativePasswordAuth(
		InitialSessionInfo si_, ChannelPromise chp_
	) {
		si = si_;
		chp = chp_;
		if (si.config.containsKey(Config.Key.PASSWORD)) {
			si.clientCaps |= ClientCapability.SECURE_CONNECTION.mask();
			if (ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(
				si.serverCaps
			))
				si.clientCaps
				|= ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.mask();
		}
	}

	@Override
	public boolean emitClientMessage(
		ByteBuf dst, ChannelHandlerContext ctx
	) {
		Config cfg = si.config;
		dst.writeIntLE(
			(int)(si.clientCaps & 0xffffffff)
		);

		dst.writeIntLE(si.packetSize);
		dst.writeShortLE(si.encoding.mysqlId);
		dst.writeZero(22);

		dst.writeBytes(cfg.getOrDefault(Config.Key.USER, "").getBytes(
			si.charset()
		));
		dst.writeByte(0);

		if (
			ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(
				si.clientCaps
			) || ClientCapability.SECURE_CONNECTION.get(
				si.clientCaps
			)
		) {
			dst.writeByte(20);
			Scramble411.encode(dst, cfg.getOrDefault(
				Config.Key.PASSWORD, ""
			).getBytes(si.charset()), si.secret);
		} else
			dst.writeByte(0);

		if (!si.schema.isEmpty()) {
			dst.writeBytes(si.schema.getBytes(si.charset()));
			dst.writeByte(0);
		}

		dst.writeBytes(AUTH_PLUGIN_NAME.getBytes(
			si.charset()
		));
		dst.writeByte(0);

		if (si.attrBuf != null) {
			Packet.writeIntLenenc(
				dst, si.attrBuf.readableBytes()
			);
			dst.writeBytes(si.attrBuf);
			si.attrBuf.release();
			si.attrBuf= null;
		}

		return false;
	}

	@Override
	public void acceptServerMessage(
		ByteBuf src, ChannelHandlerContext ctx
	) {
		si.seqNum = Packet.getSeqNum(src) + 1;
		src.skipBytes(Packet.HEADER_SIZE);

		int type = Packet.readInt1(src);

		switch (type) {
		case Packet.OK:
			try {
				si.onAuth(src, ctx, chp);
			} catch (Exception e) {
				Channels.discardActiveDictum(
					ctx.channel(), e
				);
			}
			break;
		case Packet.EOF:
			authSwitch(src, ctx);
			break;
		case Packet.ERR:
			Channels.discardActiveDictum(
				ctx.channel(),
				Packet.parseError(
					src, si.charset()
				)
			);
			return;
		default:
			Channels.discardActiveDictum(
				ctx.channel(),
				new DecoderException(
					"unsupported packet type "
					+ Integer.toString(type)
				)
			);
		}
	}

	private void authSwitch(ByteBuf src, ChannelHandlerContext ctx) {
		String authPluginName;
		byte[] pluginData = null;

		if (!src.isReadable())
			authPluginName = "mysql_old_password";
		else {
			authPluginName = Packet.readStringNT(
				src, si.charset()
			);
			pluginData = new byte[src.readableBytes()];
			src.readBytes(pluginData);
		}

		Channel ch = ctx.channel();

		switch (authPluginName) {
		case "sha256_password":
			si.secret = pluginData;
			Channels.discardActiveDictum(ch);
			ch.writeAndFlush(
				new Sha256PasswordAuth(si, chp)
			).addListener(Channels::defaultSendListener);
		default:
			Channels.discardActiveDictum(ch, Packet.makeError(
				ErrorNumbers.ER_NOT_SUPPORTED_AUTH_MODE,
				authPluginName
			));
		}
	}

	public void handleFailure(Throwable cause) {
		chp.setFailure(cause);
	}

	@Override
	public int getSeqNum() {
		return si.seqNum;
	}

	public static String AUTH_PLUGIN_NAME = "mysql_native_password";

	private final InitialSessionInfo si;
	private ChannelPromise chp;
}
