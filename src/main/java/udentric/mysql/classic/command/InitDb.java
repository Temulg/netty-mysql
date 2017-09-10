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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.classic.CharsetInfo;
import udentric.mysql.classic.Fields;
import udentric.mysql.classic.MySQLException;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResponseType;
import udentric.mysql.classic.Session;

public class InitDb implements Any {
	public InitDb(String catalog_, CharsetInfo.Entry charset_) {
		catalog = catalog_;
		charset = charset_;
	}

	@Override
	public void encode(ByteBuf dst, Session ss) {
		dst.writeByte(OPCODE);
		dst.writeCharSequence(catalog, charset.javaCharset);
	}

	@Override
	public void handleReply(
		ByteBuf src, Session ss, ChannelHandlerContext ctx
	) {
		int nextSeqNum = Packet.getSeqNum(src);
		if (nextSeqNum != 1) {
			ss.discardCommand(new DecoderException(
				"unexpected seqNum"
			));
			return;
		}

		src.skipBytes(Packet.HEADER_SIZE);

		int type = Fields.readInt1(src);
		switch (type) {
		case ResponseType.OK:
			try {
				Packet.OK ok = new Packet.OK(src, charset);
				Session.LOGGER.debug(
					"catalog changed: {}", ok.info
				);
				ss.setCatalog(catalog);
				ss.discardCommand();
				if (chp != null)
					chp.setSuccess();
			} catch (Exception e) {
				ss.discardCommand(e);
			}
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

	@Override
	public void handleFailure(Throwable cause) {
		if (chp != null)
			chp.setFailure(cause);
	}

	@Override
	public ChannelPromise channelPromise() {
		return chp;
	}

	@Override
	public InitDb withChannelPromise(ChannelPromise chp_) {
		chp = chp_;
		return this;
	}

	public static final int OPCODE = 2;

	private final String catalog;
	private final CharsetInfo.Entry charset;
	private ChannelPromise chp;
}
