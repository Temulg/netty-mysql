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

package udentric.mysql.classic.command;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.Config;
import udentric.mysql.classic.CharsetInfo;
import udentric.mysql.classic.ClientCapability;
import udentric.mysql.classic.Fields;
import udentric.mysql.classic.MySQLException;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResponseType;
import udentric.mysql.classic.Session;
import udentric.mysql.util.Scramble411;

public class MysqlNativePasswordAuth implements Any {
	public MysqlNativePasswordAuth(
		int seqNum_, long caps_,
		byte[] secret_,
		int maxPacketSize_,
		CharsetInfo.Entry charset_,
		ByteBuf attrBuf_
	) {
		seqNum = seqNum_;
		caps = caps_;
		secret = secret_;
		maxPacketSize = maxPacketSize_;
		charset = charset_;
		attrBuf = attrBuf_;
	}

	public void encode(ByteBuf dst, Session ss) {
		Config cfg = ss.getConfig();
		dst.writeIntLE(
			(int)(caps & 0xffffffff)
		);

		dst.writeIntLE(maxPacketSize);
		dst.writeByte(charset.id);
		dst.writeZero(23);

		dst.writeBytes(cfg.getOrDefault(Config.Key.user, "").getBytes(
			charset.javaCharset
		));
		dst.writeByte(0);

		if (
			ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(
				caps
			) || ClientCapability.SECURE_CONNECTION.get(caps)
		) {
			dst.writeByte(20);
			Scramble411.encode(dst, cfg.getOrDefault(
				Config.Key.password, ""
			).getBytes(charset.javaCharset), secret);
		} else
			dst.writeByte(0);

		dst.writeBytes(AUTH_PLUGIN_NAME.getBytes(
			charset.javaCharset
		));
		dst.writeByte(0);

		if (attrBuf != null) {
			Fields.writeIntLenenc(dst, attrBuf.readableBytes());
			dst.writeBytes(attrBuf);
			attrBuf.release();
		}
	}

	public void handleReply(
		ByteBuf src, Session ss, ChannelHandlerContext ctx
	) {
		int nextSeqNum = Packet.getSeqNum(src);
		src.skipBytes(Packet.HEADER_SIZE);

		int type = Fields.readInt1(src);

		System.err.format("--7- resp reply type %x\n", type);
		switch (type) {
		case ResponseType.OK:
			try {
				Packet.OK ok = new Packet.OK(src, charset);
				Session.LOGGER.debug(
					"authenticated: {}", ok.info
				);
				ss.discardCommand();
				if (chp != null)
					chp.setSuccess();
			} catch (Exception e) {
				ss.discardCommand(e);
			}
			break;
		case ResponseType.EOF:
			authSwitch(src, ss);
			break;
		case ResponseType.ERR:
			ss.discardCommand(MySQLException.fromErrPacket(src));
			return;
		default:
			ss.discardCommand(new DecoderException(
				"unsupported packet type "
				+ Integer.toString(type)
			));
			return;
		}
	}

	private void authSwitch(ByteBuf src, Session ss) {
		ss.discardCommand();
		if (!src.isReadable()) {
			handleFailure(new DecoderException(
				"mysql_old_password auth method not supported"
			));
			return;
		}

		String authPluginName = Fields.readStringNT(
			src, charset.javaCharset
		);
		byte[] pluginData = Fields.readBytes(src, src.readableBytes());
		handleFailure(new DecoderException(String.format(
			"%s auth method not supported (data: %s)",
			authPluginName, Arrays.toString(pluginData)
		)));
	}

	public void handleFailure(Throwable cause) {
		if (chp != null)
			chp.setFailure(cause);
	}

	@Override
	public int getSeqNum() {
		return seqNum;
	}

	@Override
	public ChannelPromise channelPromise() {
		return chp;
	}

	@Override
	public MysqlNativePasswordAuth withChannelPromise(
		ChannelPromise chp_
	) {
		chp = chp_;
		return this;
	}

	public static String AUTH_PLUGIN_NAME = "mysql_native_password";

	private final int seqNum;
	private final long caps;
	private final byte[] secret;
	private final int maxPacketSize;
	private final CharsetInfo.Entry charset;
	private final ByteBuf attrBuf;
	private ChannelPromise chp;
}
