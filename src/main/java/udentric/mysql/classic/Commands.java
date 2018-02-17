/*
 * Copyright (c) 2017 - 2018 Alex Dubov <oakad@yahoo.com>
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

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import udentric.mysql.ServerAck;
import udentric.mysql.DataRow;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.dicta.PrepareStatement;
import udentric.mysql.classic.dicta.ExecuteStatement;
import udentric.mysql.classic.dicta.Query;

class Commands implements udentric.mysql.Commands {
	Commands(Channel ch_) {
		ch = ch_;
	}

	@Override
	public Future<udentric.mysql.ServerAck> executeUpdate(String sql) {
		Promise<
			udentric.mysql.ServerAck
		> sp = Channels.newServerPromise(ch);
		ch.writeAndFlush(new Query(sql, new ResultSetConsumer() {
			@Override
			public void acceptFailure(Throwable cause) {
				sp.setFailure(cause);
			}
		
			@Override
			public void acceptAck(
				ServerAck ack, boolean terminal
			) {
				if (terminal)
					sp.setSuccess(ack);
			}
		})).addListener(Channels::defaultSendListener);
		return sp;
	}

	@Override
	public Future<PreparedStatement> prepareStatement(String sql) {
		DefaultPromise<
			PreparedStatement
		> psp = new DefaultPromise<>(ch.eventLoop());

		ch.writeAndFlush(new PrepareStatement(sql, psp)).addListener(
			Channels::defaultSendListener
		);

		return psp;
	}

	@Override
	public Future<udentric.mysql.ServerAck> executeUpdate(
		PreparedStatement pstmt, Object... args
	) {
		Promise<
			udentric.mysql.ServerAck
		> sp = Channels.newServerPromise(ch);
		ch.writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer() {
				@Override
				public void acceptFailure(Throwable cause) {
					sp.setFailure(cause);
				}

				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					if (terminal)
						sp.setSuccess(ack);
				}
			},
			args
		)).addListener(Channels::defaultSendListener);
		return sp;
	}

	@Override
	public Future<List<?>> selectColumn(String sql, int colNum) {
		Promise<List<?>> sp = new DefaultPromise<List<?>>(
			ch.eventLoop()
		);

		ch.writeAndFlush(new Query(sql, new ResultSetConsumer() {
			@Override
			public void acceptRow(DataRow row) {
				result.add(row.getValue(colNum));
			}

			@Override
			public void acceptFailure(Throwable cause) {
				sp.setFailure(cause);
			}
	
			@Override
			public void acceptAck(
				ServerAck ack, boolean terminal
			) {
				if (terminal)
					sp.setSuccess(result);
			}

			final ArrayList<?> result = new ArrayList<>();
		})).addListener(Channels::defaultSendListener);
		return sp;
	}

	private final Channel ch;
}
