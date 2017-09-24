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
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.dicta.PrepareStatement;
import udentric.mysql.classic.dicta.ExecuteStatement;
import udentric.mysql.classic.dicta.Query;

public class Commands {
	private Commands() {
	}

	public static Future<Packet.ServerAck> executeUpdate(
		Channel ch, String sql
	) {
		Promise<Packet.ServerAck> sp = Channels.newServerPromise(ch);
		ch.writeAndFlush(new Query(sql, new ResultSetConsumer(){
			@Override
			public void acceptRow(Row row) {}
		
			@Override
			public void acceptMetadata(ColumnDefinition colDef) {}
		
			@Override
			public void acceptFailure(Throwable cause) {
				sp.setFailure(cause);
			}
		
			@Override
			public void acceptAck(
				Packet.ServerAck ack, boolean terminal
			) {
				if (terminal)
					sp.setSuccess(ack);
			}
		})).addListener(Channels::defaultSendListener);
		return sp;
	}

	public static Future<PreparedStatement> prepareStatement(
		Channel ch, String sql
	) {
		Future<PreparedStatement> psf = ch.attr(
			Channels.PSTMT_TRACKER
		).get().beginPrepare(ch, sql);
		if (psf.isDone())
			return psf;


		ch.writeAndFlush(new PrepareStatement(sql, psp)).addListener(
			Channels::defaultSendListener
		);

		return psf;
	}

	public static Future<Packet.ServerAck> executeUpdate(
		Channel ch, PreparedStatement pstmt, Object... args
	) {
		Promise<Packet.ServerAck> sp = Channels.newServerPromise(ch);
		ch.writeAndFlush(new ExecuteStatement(
			pstmt, new ResultSetConsumer(){
				@Override
				public void acceptRow(Row row) {}
		
				@Override
				public void acceptMetadata(ColumnDefinition colDef) {}
		
				@Override
				public void acceptFailure(Throwable cause) {
					sp.setFailure(cause);
				}
		
				@Override
				public void acceptAck(
					Packet.ServerAck ack, boolean terminal
				) {
					if (terminal)
						sp.setSuccess(ack);
				}
			},
			args
		)).addListener(Channels::defaultSendListener);
		return sp;
	}
}
