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
 * May contain portions of MySQL Connector/J testsuite
 *
 * Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * The MySQL Connector/J is licensed under the terms of the GPLv2
 * <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL
 * Connectors. There are special exceptions to the terms and conditions of
 * the GPLv2 as it is applied to this software, see the FOSS License Exception
 * <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
 */

package testsuite;

import io.netty.bootstrap.Bootstrap;
import java.sql.SQLException;
import java.util.ArrayDeque;

import org.testng.ITestContext;
import org.testng.annotations.AfterMethod;
import org.testng.log4testng.Logger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import org.testng.TestRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import udentric.mysql.Config;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.SyncCommands;

public abstract class TestCase {
	protected TestCase(Logger logger_) {
		logger = logger_;
	}

	@BeforeClass
	public void beforeClass(ITestContext ctx) {
		TestRunner runner = (TestRunner)ctx;
		runner.setVerbose(0);

		ResourceLeakDetector.setLevel(Level.PARANOID);
		grp = new NioEventLoopGroup();
	}

	@AfterClass
	public void afterClass(ITestContext ctx) throws Exception {
		grp.shutdownGracefully().await();
		grp = null;
	}

	protected boolean versionMeetsMinimum(int major, int minor) {
		return versionMeetsMinimum(major, minor, 0);
	}

	protected boolean versionMeetsMinimum(
		int major, int minor, int subminor
	) {
		return versionMeetsMinimum(channel(), major, minor, subminor);
	}

	protected static boolean versionMeetsMinimum(
		Channel ch, int major, int minor, int subminor
	) {
		return Channels.sessionInfo(ch).version.meetsMinimum(
			major, minor, subminor
		);
	}

	protected Channel channel() {
		if (channel != null) {
			return channel;
		}

		ChannelFuture chf = Channels.create(
			Config.fromEnvironment(true).withValue(
				Config.Key.DBNAME, "testsuite"
			).build(),
			(new Bootstrap()).group(
				new NioEventLoopGroup()
			).channel(NioSocketChannel.class)
		);

		try {
			chf.await();
		} catch (InterruptedException e) {
			Channels.throwAny(e);
		}

		if (chf.isSuccess()) {
			channel = chf.channel();
			return channel;
		} else
			Channels.throwAny(chf.cause());

		return null;
	}

	protected void createTable(
		Channel ch, String tableName, String columnsAndOtherStuff
	) throws SQLException {
		createSchemaObject(
			ch, "TABLE", tableName, columnsAndOtherStuff
		);
	}

	protected void createTable(
		String tableName, String columnsAndOtherStuff
	) throws SQLException {
		createTable(channel(), tableName, columnsAndOtherStuff);
	}

	protected class SchemaObject {
		SchemaObject(
			Channel ch, String type_, String name_
		) throws SQLException {
			type = type_;
			name = name_;

			try {
				drop(ch);
			} catch (SQLException e) {
				if (!e.getMessage().startsWith(
					"Operation DROP USER failed"
				))
					throw e;
			}
		}

		void drop(Channel ch) throws SQLException {
			if (
				!type.equalsIgnoreCase("USER")
				|| versionMeetsMinimum(ch, 5, 7, 8)
			) {
				SyncCommands.executeUpdate(ch, String.format(
					"DROP %s IF EXISTS %s",
					type, name
				));
			} else {
				SyncCommands.executeUpdate(ch, String.format(
					"DROP %s %s", type, name
				));
			}
			SyncCommands.executeUpdate(ch, "flush privileges");
		}

		final String type;
		final String name;
	}

	protected void createSchemaObject(
		Channel ch, String objectType, String objectName,
		String columnsAndOtherStuff
	) throws SQLException {
		createdObjects.offerLast(
			new SchemaObject(ch, objectType, objectName)
		);

		String sql = String.format(
			"CREATE  %s %s %s", objectType, objectName,
			columnsAndOtherStuff
		);

		try {
			SyncCommands.executeUpdate(ch, sql);
		} catch (Exception e_) {
			SQLException e;

			if (e_ instanceof SQLException)
				e = (SQLException)e_;
			else
				throw e_;

			if ("42S01".equals(e.getSQLState())) {
				logger.warn(
					"Stale mysqld table cache preventing "
					+ "table creation - flushing tables "
					+ "and trying again"
				);
				SyncCommands.executeUpdate(ch, "FLUSH TABLES");
				SyncCommands.executeUpdate(ch, sql);
			} else {
				throw e;
			}
		}
	}

	/*
	@BeforeMethod
	public void setUp() {

		conn = DriverManager.getConnection(dbUrl, props);

		//this.sha256Conn = sha256Url == null ? null : DriverManager.getConnection(sha256Url, props);

		serverVersion = conn.getServerVersion();

		stmt = conn.createStatement();

		try {
			if (dbUrl.indexOf("mysql") != -1) {
				this.rs = this.stmt.executeQuery("SELECT VERSION()");
				this.rs.next();
				logDebug("Connected to " + this.rs.getString(1));
			} else {
				logDebug("Connected to " + this.conn.getMetaData().getDatabaseProductName() + " / " + this.conn.getMetaData().getDatabaseProductVersion());
			}
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
		}

		this.isOnCSFS = !this.conn.getMetaData().storesLowerCaseIdentifiers();

		if (this.sha256Conn != null) {
			this.sha256Stmt = this.sha256Conn.createStatement();

			try {
				if (sha256Url.indexOf("mysql") != -1) {
					this.sha256Rs = this.sha256Stmt.executeQuery("SELECT VERSION()");
					this.sha256Rs.next();
					logDebug("Connected to " + this.sha256Rs.getString(1));
				} else {
					logDebug("Connected to " + this.sha256Conn.getMetaData().getDatabaseProductName() + " / "
					+ this.sha256Conn.getMetaData().getDatabaseProductVersion());
				}
			} finally {
				if (this.sha256Rs != null) {
					this.sha256Rs.close();
					this.sha256Rs = null;
				}
			}
		}
	}
	*/

	@AfterMethod
	public void tearDown() throws Exception {
/*
		if (System.getProperty(
			PropertyDefinitions.SYSP_testsuite_retainArtifacts
		) == null) {
			Statement st = this.conn == null || this.conn.isClosed() ? getNewConnection().createStatement() : this.conn.createStatement();
			Statement sha256st;
			if (this.sha256Conn == null || this.sha256Conn.isClosed()) {
				Connection c = getNewSha256Connection();
				sha256st = c == null ? null : c.createStatement();
			} else {
				sha256st = this.sha256Conn.createStatement();
			}

			for (int i = 0; i < this.createdObjects.size(); i++) {
				String[] objectInfo = this.createdObjects.get(i);

				try {
					dropSchemaObject(st, objectInfo[0], objectInfo[1]);
				} catch (SQLException SQLE) {}

				try {
					dropSchemaObject(sha256st, objectInfo[0], objectInfo[1]);
				} catch (SQLException SQLE) {}
			}

			st.close();
			if (sha256st != null) {
				sha256st.close();
			}
		}
*/
		if (channel != null) {
			channel.close().await();
			channel = null;
		}
	}

	private final ArrayDeque<
		SchemaObject
	> createdObjects = new ArrayDeque<>();
	protected final Logger logger;
	protected NioEventLoopGroup grp;
	protected Channel channel;
}
