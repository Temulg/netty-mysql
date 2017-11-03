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

package udentric.mysql.classic.prepared.singular;

import java.util.HashMap;
import java.util.concurrent.locks.StampedLock;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import udentric.mysql.FieldSet;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.FieldSetImpl;

public class StatementTracker
implements udentric.mysql.classic.prepared.StatementTracker {
	@Override
	public Promise<PreparedStatement> beginPrepare(Channel ch, String sql) {
		long stamp = lock.readLock();


		Promise<PreparedStatement> psp = prepareIfAvailable(ch, sql);
		if (psp != null) {
			lock.unlockRead(stamp);
			return psp;
		}

		stamp = lock.tryConvertToWriteLock(stamp);
		if (stamp == 0) {
			lock.unlockRead(stamp);
			stamp = lock.writeLock();

			psp = prepareIfAvailable(
				ch, sql
			);
			if (psp != null) {
				lock.unlockWrite(stamp);
				return psp;
			}
		}

		Placeholder p = new Placeholder(sql, ch);
		sqlToStmt.put(sql, p);
		lock.unlockWrite(stamp);
		return p.psp;
	}

	private Promise<PreparedStatement> prepareIfAvailable(
		Channel ch, String sql
	) {
		PreparedStatement s = sqlToStmt.get(sql);
		if (s != null) {
			if (s instanceof Placeholder) {
				return ((Placeholder)s).psp;
			} else {
				return ch.eventLoop().<
					PreparedStatement
				>newPromise().setSuccess(s);
			}
		} else
			return null;
	}

	@Override
	public void completePrepare(
		String sql, int remoteId, FieldSet parameters,
		FieldSet columns
	) {
		long stamp = lock.writeLock();
		Statement s = new Statement(
			sql, remoteId, (FieldSetImpl)parameters,
			(FieldSetImpl)columns
		);
		Placeholder p = (Placeholder)sqlToStmt.get(sql);

		idToStmt.put(remoteId, s);
		sqlToStmt.put(sql, s);
		lock.unlockWrite(stamp);
		p.psp.setSuccess(s);
	}

	@Override
	public void discard(PreparedStatement stmt_) {
		Statement stmt = (Statement)stmt_;

		long stamp = lock.writeLock();
		idToStmt.remove(stmt.srvId);
		sqlToStmt.remove(stmt.sql);
		lock.unlockWrite(stamp);
	}


	class Placeholder implements PreparedStatement {
		private Placeholder(String sql_, Channel ch) {
			sql = sql_;
			psp = new DefaultPromise<>(ch.eventLoop());
			psp.addListener(this::discardFailed);
		}

		@Override
		public FieldSet parameters() {
			return null;
		}

		@Override
		public FieldSet columns() {
			return null;
		}

		private void discardFailed(Future<?> f) {
			if (f.isSuccess())
				return;

			long stamp = lock.writeLock();
			sqlToStmt.computeIfPresent(sql, (k, v) -> {
				return (v == this) ? null : v;
			});
			lock.unlockWrite(stamp);
		}

		private final String sql;
		private final DefaultPromise<PreparedStatement> psp;
	}

	private final StampedLock lock = new StampedLock();
	private final HashMap<
		Integer, PreparedStatement
	> idToStmt = new HashMap<>();
	private final HashMap<
		String, PreparedStatement
	> sqlToStmt = new HashMap<>();
}