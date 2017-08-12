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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Session {
	Session() {
		logger = LogManager.getLogger(Session.class);
	}

	public void setServerIdentity(
		CharSequence srvName_, int srvConnId_
	) {
		srvName = srvName_.toString();
		srvConnId = srvConnId_;
		logger.debug(
			"server identity set: {} ({})", srvConnId, srvName
		);
	}

	public void setServerCharsetId(int charsetId) {
	}

	public void setServerCapabilities(long caps) {
		logger.debug(ClientCapability.describe(caps));
		serverCaps = caps;

		clientCaps = ClientCapability.PLUGIN_AUTH.mask()
			| ClientCapability.LONG_PASSWORD.mask()
			| ClientCapability.PROTOCOL_41.mask()
			| ClientCapability.TRANSACTIONS.mask()
			| ClientCapability.MULTI_RESULTS.mask()
			| ClientCapability.SECURE_CONNECTION.mask()
			| ClientCapability.MULTI_STATEMENTS.mask();

		if (ClientCapability.CAN_HANDLE_EXPIRED_PASSWORDS.get(serverCaps))
			clientCaps |= ClientCapability.CAN_HANDLE_EXPIRED_PASSWORDS.mask();

		if (ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(serverCaps))
			clientCaps |= ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.mask();
	}

	public long getClientCapabilities() {
		return clientCaps;
	}

	public final Logger logger;
	private String srvName;
	private int srvConnId;
	private long serverCaps;
	private long clientCaps;
}
