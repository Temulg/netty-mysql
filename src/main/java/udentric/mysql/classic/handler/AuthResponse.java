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

package udentric.mysql.classic.handler;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import udentric.mysql.classic.ClientCapability;
import udentric.mysql.classic.Fields;
import udentric.mysql.classic.MessageHandler;
import udentric.mysql.classic.MySQLException;
import udentric.mysql.classic.ProtocolHandler;
import udentric.mysql.classic.ResponseType;
import udentric.mysql.classic.ServerStatus;
import udentric.mysql.util.ByteString;

public class AuthResponse implements MessageHandler {
	private AuthResponse() {}

	@Override
	public void process(ProtocolHandler ph, ByteBuf msg) {
		int type = msg.readByte();

		switch (type) {
		case ResponseType.OK:
			okReceived(ph, msg);
			break;
		case ResponseType.EOF:
			authSwitch(ph, msg);
			break;
		case ResponseType.ERR:
			throw MySQLException.fromErrPacket(msg);
		default:
			throw new DecoderException(
				"unsupported packet type "
				+ Integer.toString(type)
			);
		}

		/*
		int<1> 0x00 : OK_Packet header or (0xFE if CLIENT_DEPRECATE_EOF is set)
int<lenenc> affected rows
int<lenenc> last insert id
int<2> server status
int<2> warning count
if session_tracking_supported (see CLIENT_SESSION_TRACK)

    string<lenenc> info
    if (status flags & SERVER_SESSION_STATE_CHANGED)
        string<lenenc> session state info
        string<lenenc> value of variable 

else

    string<EOF> info
    */
	}

	private void okReceived(ProtocolHandler ph, ByteBuf msg) {
		long rows = Fields.readLongLenenc(msg);
		long insertId = Fields.readLongLenenc(msg);
		short srvStatus = msg.readShortLE();
		int warnCount = Fields.readInt2(msg);
		ByteString info;

		if (ClientCapability.SESSION_TRACK.get(
			ph.getServerCapabilities()
		)) {
			info = Fields.readStringLenenc(msg);
			if (ServerStatus.SESSION_STATE_CHANGED.get(
				srvStatus
			)) {
				throw new DecoderException(
					"session state info not decoded"
				);
			}
		} else {
			info = new ByteString(msg, msg.readableBytes());
		}

		ph.logger.debug(
			"successfully connected to MySQL server (%s)", info
		);

		ph.setMessageHandler(null);
	}

	private void authSwitch(ProtocolHandler ph, ByteBuf msg) {
		if (!msg.isReadable()) {
			throw new DecoderException(
				"mysql_old_password auth method not supported"
			);
		}

		ByteString authPluginName = Fields.readStringNT(msg);
		byte[] pluginData = Fields.readBytes(msg, msg.readableBytes());
		throw new DecoderException(String.format(
			"%s auth method not supported (data: %s)",
			authPluginName, Arrays.toString(pluginData)
		));
	}

	public static final AuthResponse INSTANCE = new AuthResponse();
}
