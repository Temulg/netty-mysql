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

package udentric.mysql.classic;

import java.nio.charset.Charset;

import udentric.mysql.Config;
import udentric.mysql.Encoding;
import udentric.mysql.ServerVersion;

public class SessionInfo {
	SessionInfo(InitialSessionInfo si) {
		config = si.config;
		version = si.version;
		encoding = si.encoding;
		serverCaps = si.serverCaps;
		clientCaps = si.clientCaps;
		srvConnId = si.srvConnId;
		packetSize = si.packetSize;
	}

	public boolean expectEof() {
		return !ClientCapability.DEPRECATE_EOF.get(clientCaps);
	}

	public Charset charset() {
		return encoding.charset;
	}

	public final Config config;
	public final ServerVersion version;
	public final Encoding encoding;
	public final long serverCaps;
	public final long clientCaps;
	public final int srvConnId;
	public final int packetSize;
}
