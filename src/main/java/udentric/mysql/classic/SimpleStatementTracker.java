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

import java.util.HashMap;
import java.util.concurrent.locks.StampedLock;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;

class SimpleStatementTracker implements PreparedStatementTracker {
	public Promise prepare(Channel ch, String sql) {
		long stamp = lock.readLock();
		Stmt s = sqlToStmt.computeIfAbsent(sql, sql_ -> {
			return new Stmt(sql_, ch);
		});

		if (s.id == STMT_ID_NEW) {
		}

		lock.unlockRead(stamp);
		return s.regPromise;
	}

	@Override
	public int registerStatement(String sql, int remoteId) {
		stmtMap.put(remoteId, sql);
		return remoteId;
	}

	private static class Stmt {
		Stmt(String sql_, Channel ch) {
			sql = sql_;
			regPromise = new DefaultPromise(ch.eventLoop());
		}

		final String sql;
		final Promise regPromise;
		int id = STMT_ID_NEW;
	}

	private static final int STMT_ID_NEW = -2;
	private static final int STMT_ID_IN_PROGRESS = -1;

	private final StampedLock lock = new StampedLock();
	private final HashMap<Integer, Stmt> idToStmt = new HashMap<>();
	private final HashMap<String, Stmt> sqlToStmt = new HashMap<>();
}
