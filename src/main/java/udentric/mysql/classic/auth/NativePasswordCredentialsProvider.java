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
import udentric.mysql.util.ByteString;
import udentric.mysql.util.Scramble411;

public class NativePasswordCredentialsProvider implements CredentialsProvider {
	public NativePasswordCredentialsProvider() {
		String user = System.getProperty("udentric.mysql.user");
		if (user == null) {
			user = System.getenv("USER");
		}

		String pwd = System.getProperty("udentric.mysql.password");
		if (pwd == null) {
			pwd = System.getenv("MYSQL_PWD");
		}

		username = user != null
			? user.getBytes(StandardCharsets.UTF_8)
			: new byte[0];
		password = pwd != null
			? pwd.getBytes(StandardCharsets.UTF_8)
			: new byte[0];
	}

	public NativePasswordCredentialsProvider(
		byte[] username_, byte[] password_
	) {
		username = username_;
		password = password_;
	}

	@Override
	public void updateAuthMessage(
		ProtocolHandler ph, ByteBuf msg, ByteString authPluginName,
		byte[] secret
	) {
		if (!AUTH_PLUGIN_NAME.equals(authPluginName)) {
			throw new IllegalStateException(
				"unsupported auth plugin " + authPluginName
			);
		}

		long caps = ph.getClientCapabilities();

		msg.writeBytes(username);
		msg.writeByte(0);

		if (
			ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(
				caps
			) || ClientCapability.SECURE_CONNECTION.get(caps)
		) {
			msg.writeByte(20);
			Scramble411.encode(msg, password, secret);
		} else
			msg.writeByte(0);

		msg.writeBytes(authPluginName.getBytes());
		msg.writeByte(0);
		ph.setMessageHandler(AuthResponse.INSTANCE);
	}

	@Override
	public boolean isSecure() {
		return password.length > 0;
	}

	public static ByteString AUTH_PLUGIN_NAME = new ByteString(
		"mysql_native_password"
	);

	private final byte[] username;
	private final byte[] password;
}
