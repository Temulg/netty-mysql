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

import io.netty.buffer.ByteBuf;
import udentric.mysql.classic.MessageHandler;
import udentric.mysql.classic.ProtocolHandler;

public class AuthResponse implements MessageHandler {
	private AuthResponse() {}

	@Override
	public void process(ProtocolHandler ph, ByteBuf msg) {
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

	public static final AuthResponse INSTANCE = new AuthResponse();
}
