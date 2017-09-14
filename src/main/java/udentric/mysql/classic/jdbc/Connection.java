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

package udentric.mysql.classic.jdbc;

import com.google.common.collect.Lists;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Promise;

import java.util.IdentityHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import udentric.mysql.Config;
import udentric.mysql.ServerVersion;
import udentric.mysql.classic.Client;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.SessionInfo;
import udentric.mysql.classic.dicta.InitDb;
import udentric.mysql.classic.dicta.Quit;
import udentric.mysql.util.QueryNormalizer;

public class Connection implements java.sql.Connection {
	public Connection(ChannelFuture chf) throws SQLException {
		try {
			if (!chf.await().isSuccess()) {
				throw chf.cause();
			}
		} catch (Throwable t) {
			Client.throwAny(t);
		}

		LOGGER.debug("connection established");

		ch = chf.channel();
		si = ch.attr(Client.SESSION_INFO).get();

		schema = si.cl.getConfig().getOrDefault(
			Config.Key.DBNAME, ""
		);
		if (!schema.isEmpty())
			setCatalog(schema);
	}

	public ServerVersion getServerVersion() {
		return si.version;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return createStatement(
			ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY
		);
	}

	@Override
	public PreparedStatement prepareStatement(
		String sql
	) throws SQLException {
		return null;
	}

	@Override
	public CallableStatement prepareCall(
		String sql
	) throws SQLException {
		return null;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		QueryNormalizer qn = new QueryNormalizer(
			sql, null, true
		);

		return qn.normalize().toString();
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return true;
	}

	@Override
	public void commit() throws SQLException {
	}

	@Override
	public void rollback() throws SQLException {
	}

	@Override
	public void close() throws SQLException {
		closeAllStatements();

		try {
			if (ch.isOpen()) {
				ch.writeAndFlush(Quit.INSTANCE).await();
				ch.close().await();
			}
		} catch (InterruptedException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return !ch.isOpen();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return null;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		Promise<Packet.ServerAck> sp = Client.newServerPromise(ch);

		ch.writeAndFlush(new InitDb(catalog, sp)).addListener(
			Client::defaultSendListener
		);

		try {
			sp.await();
		} catch (InterruptedException e) {
			Client.discardActiveDictum(ch, e);
		}

		if (!sp.isSuccess()) {
			Client.throwAny(sp.cause());
		} else {
			Packet.ServerAck ack = sp.getNow();
			if (ack.warnCount > 0)
				LOGGER.warn(
					"{} warning(s) raised", ack.warnCount
				);

			schema = catalog;
		}
	}

	@Override
	public String getCatalog() {
		return schema;
	}

	public Channel getChannel() {
		return ch;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public synchronized Statement createStatement(
		int resultSetType, int resultSetConcurrency
	) throws SQLException {
		Statement stmt = new Statement(
			this, resultSetType, resultSetConcurrency
		);
		statements.put(stmt, Boolean.TRUE);
		return stmt;
	}

	@Override
	public PreparedStatement prepareStatement(
		String sql, int resultSetType, int resultSetConcurrency
	) throws SQLException {
		return null;
	}

	@Override
	public CallableStatement prepareCall(
		String sql, int resultSetType, int resultSetConcurrency
	) throws SQLException {
		return null;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return null;
	}

	@Override
	public void setTypeMap(
		Map<String, Class<?>> map
	) throws SQLException {
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
	}

	@Override
	public int getHoldability() throws SQLException {
		return 0;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return null;
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return null;
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
	}

	@Override
	public Statement createStatement(
		int resultSetType, int resultSetConcurrency,
		int resultSetHoldability
	) throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(
		String sql, int resultSetType, int resultSetConcurrency,
		int resultSetHoldability
	) throws SQLException {
		return null;
	}

	@Override
	public CallableStatement prepareCall(
		String sql, int resultSetType, int resultSetConcurrency,
		int resultSetHoldability
	) throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(
		String sql, int autoGeneratedKeys
	) throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(
		String sql, int columnIndexes[]
	) throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(
		String sql, String columnNames[]
	) throws SQLException {
		return null;
	}


	@Override
	public Clob createClob() throws SQLException {
		return null;
	}


	@Override
	public Blob createBlob() throws SQLException {
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return null;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return true;
	}

	@Override
	public void setClientInfo(
		String name, String value
	) throws SQLClientInfoException {
	}

	@Override
	public void setClientInfo(
		Properties properties
	) throws SQLClientInfoException {
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return null;
	}


	@Override
	public Properties getClientInfo() throws SQLException {
		return null;
	}

	@Override
	public Array createArrayOf(
		String typeName, Object[] elements
	) throws SQLException {
		return null;
	}

	@Override
	public Struct createStruct(
		String typeName, Object[] attributes
	) throws SQLException {
		return null;
	}

	@Override
	public void setSchema(String schema) throws SQLException {
	}

	@Override
	public String getSchema() throws SQLException {
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
	}


	@Override
	public void setNetworkTimeout(
		Executor executor, int milliseconds
	) throws SQLException {}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return 0;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	public Config getConfig() {
		return si.cl.getConfig();
	}

	private synchronized void closeAllStatements() {
		Lists.newArrayList(statements.keySet()).forEach(stmt -> {
			try {
				stmt.close();
			} catch (SQLException e) {
				LOGGER.warn("exception closing statement", e);
			}
		});
	}

	synchronized void releaseStatement(Statement stmt) {
		statements.remove(stmt);
	}

	private static final Logger LOGGER = LogManager.getLogger(
		Connection.class
	);

	private final Channel ch;
	private final SessionInfo si;
	private final IdentityHashMap<
		Statement, Boolean
	> statements = new IdentityHashMap<>();
	private String schema;
}
