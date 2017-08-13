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

package udentric.mysql.classic.auth;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import udentric.mysql.classic.ClientCapability;
import udentric.mysql.classic.ProtocolHandler;
import udentric.mysql.classic.handler.AuthResponse;

public class NativePasswordCredentialsProvider implements CredentialsProvider {
	public NativePasswordCredentialsProvider() {
		this(System.getenv("USER"), System.getenv("MYSQL_PWD"));
	}

	public NativePasswordCredentialsProvider(
		byte[] username_, byte[] password_
	) {
		username = username_;
		password = password_;
	}

	private NativePasswordCredentialsProvider(
		String username_, String password_
	) {
		username = username_ != null
			? username_.getBytes(StandardCharsets.UTF_8)
			: new byte[0];
		password = password_ != null
			? password_.getBytes(StandardCharsets.UTF_8)
			: new byte[0];
	}

	@Override
	public void updateAuthMessage(
		ProtocolHandler ph, ByteBuf msg, String authPluginName,
		byte[] scramble
	) {
		if (!AUTH_PLUGIN_NAME.equals(authPluginName)) {
			throw new IllegalStateException(
				"unsupported auth plugin " + authPluginName
			);
		}

		long caps = ph.getServerCapabilities();

		msg.writeBytes(username);
		msg.writeByte(0);

		if (ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(caps)) {

		} else if (ClientCapability.SECURE_CONNECTION.get(caps)) {

		} else
			msg.writeByte(0);

		ph.setMessageHandler(AuthResponse.INSTANCE);
	}

	public static String AUTH_PLUGIN_NAME = "mysql_native_password";

	private final byte[] username;
	private final byte[] password;
}
