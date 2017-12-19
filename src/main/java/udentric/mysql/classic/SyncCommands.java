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

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import udentric.mysql.PreparedStatement;

public class SyncCommands {
	private SyncCommands() {
	}

	public static ServerAck executeUpdate(Channel ch, String sql) {
		Future<ServerAck> sf = null;

		try {
			sf = Commands.executeUpdate(
				ch, sql
			).await();
		} catch (InterruptedException e) {
			Channels.throwAny(e);
		}

		if (sf.isSuccess())
			return sf.getNow();
		else {
			Channels.throwAny(sf.cause());
			return null;
		}
	}

	public static PreparedStatement prepareStatement(
		Channel ch, String sql
	) {
		Future<PreparedStatement> psp = null;

		try {
			psp = Commands.prepareStatement(
				ch, sql
			).await();
		} catch (InterruptedException e) {
			Channels.throwAny(e);
		}

		if (psp.isSuccess())
			return psp.getNow();
		else {
			Channels.throwAny(psp.cause());
			return null;
		}
	}

	public static ServerAck executeUpdate(
		Channel ch, PreparedStatement pstmt, Object... args
	) {
		Future<ServerAck> sf = null;;

		try {
			sf = Commands.executeUpdate(
				ch, pstmt, args
			).await();
		} catch (InterruptedException e) {
			Channels.throwAny(e);
		}

		if (sf.isSuccess())
			return sf.getNow();
		else {
			Channels.throwAny(sf.cause());
			return null;
		}
	}
}
