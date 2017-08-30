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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
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
import udentric.mysql.Config;
import udentric.mysql.ServerVersion;
import udentric.mysql.Session;
import udentric.mysql.classic.ProtocolHandler;
import udentric.mysql.util.QueryNormalizer;

public class Connection implements java.sql.Connection {
	public Connection(ChannelFuture chf) {
		try {
			if (!chf.await().isSuccess()) {
				throw chf.cause();
			}
		} catch (Throwable t) {
			throwAny(t);
		}

		ch = chf.channel();
		ph = (ProtocolHandler)ch.pipeline().get(
			"mysql.protocol"
		);
	}

	@SuppressWarnings("unchecked")
	public static <T extends Throwable> void throwAny(
		Throwable t
	) throws T {
		throw (T)t;
	}

	public ServerVersion getServerVersion() {
		return ph.getServerVersion();
	}

	@Override
	public java.sql.Statement createStatement() throws SQLException {
		return new Statement(this);
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
			sql, null, true, null
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
		try {
			ch.close().await();
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
	}

	public String getCatalog() throws SQLException {
		return "";
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
	public Statement createStatement(
		int resultSetType, int resultSetConcurrency
	) throws SQLException {
		return null;
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
		return ph.getClient().getConfig();
	}

	public Session getSession() {
		return null;
	}

	private final Channel ch;
	private final ProtocolHandler ph;
}
