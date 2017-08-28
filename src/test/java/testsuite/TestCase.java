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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.log4testng.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import udentric.mysql.classic.Client;
import udentric.mysql.classic.auth.NativePasswordCredentialsProvider;
import udentric.mysql.classic.jdbc.Connection;

public abstract class TestCase {
	protected TestCase(Logger logger_) {
		logger = logger_;
	}

	protected boolean versionMeetsMinimum(int major, int minor) {
		return versionMeetsMinimum(major, minor, 0);
	}

	protected boolean versionMeetsMinimum(
		int major, int minor, int subminor
	) {
		return conn().getServerVersion().meetsMinimum(
			major, minor, subminor
		);
	}

	protected Connection conn() {
		return (Connection)testObjects.computeIfAbsent(TestObject.CONN, k -> {
			Client cl = Client.builder().withCredentials(
				new NativePasswordCredentialsProvider()
			).build();
			Connection c = new Connection(
				(new Bootstrap()).group(grp).channel(
					NioSocketChannel.class
				).handler(
					cl
				).connect(
					cl.remoteAddress()
				)
			);
			closeableObjects.offerFirst(c);
			return c;
		});
	}
/*
	protected Connection sha256Conn() {
	}

	protected PreparedStatement pstmt() {
	}

	protected ResultSet rs() {
	}

	protected ResultSet sha256Rs() {
	}

	protected Statement stmt() {
	}

	protected Statement sha256Stmt() {
	}
*/

	@BeforeClass
	public void setUpClass(ITestContext ctx) {
		ResourceLeakDetector.setLevel(Level.PARANOID);
		grp = new NioEventLoopGroup();
	}

	@AfterClass
	public void tearDownClass(ITestContext ctx) throws Exception {
		grp.shutdownGracefully().await();
		grp = null;
	}

	@BeforeMethod
	public void setUp() {

		/*
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
		*/
	}

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
		closeableObjects.forEach(obj -> {
			try {
				obj.close();
			} catch (Exception e) {
				logger.warn("exception on close", e);
			}
		});
		closeableObjects.clear();
		testObjects.clear();
	}

	protected enum TestObject {
		CONN,
		SHA256CONN,
		PSTMT,
		RS,
		SHA256RS,
		STMT,
		SHA256STMT;
	}

	private final ArrayList<String[]> createdObjects = new ArrayList<>();
	private final EnumMap<TestObject, Object> testObjects = new EnumMap<>(
		TestObject.class
	);
	private final ArrayDeque<
		AutoCloseable
	> closeableObjects = new ArrayDeque<>();
	protected final Logger logger;
	protected NioEventLoopGroup grp;
}
