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

package udentric.mysql.testsuite;

import io.netty.bootstrap.Bootstrap;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.function.Function;

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
import org.testng.annotations.BeforeMethod;

import udentric.mysql.Config;
import udentric.mysql.SyncCommands;
import udentric.mysql.classic.Channels;
import udentric.mysql.util.Throwables;

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

	protected static Channel makeChannel(
		Function<Config.Builder, Config.Builder
	> cc) {
		Config.Builder builder = Config.fromEnvironment(
			true
		).withValue(
			Config.Key.DBNAME, "testsuite"
		);

		ChannelFuture chf = Channels.create(
			cc.apply(builder).build(),
			(new Bootstrap()).group(
				new NioEventLoopGroup()
			).channel(NioSocketChannel.class)
		);

		try {
			chf.await();
		} catch (InterruptedException e) {
			Throwables.propagate(e);
		}

		if (chf.isSuccess())
			return chf.channel();
		else
			throw Throwables.propagate(chf.cause());
	}

	protected static Channel makeChannel() {
		return makeChannel(Function.identity());
	}

	protected Channel channel() {
		if (channel != null) {
			return channel;
		}

		channel = makeChannel();
		return channel;
	}

	protected void createTable(
		Channel ch, String tableName, String... extra
	) throws SQLException {
		switch (extra.length) {
		case 1:
			createSchemaObject(
				ch, "TABLE", tableName, extra[0]
			);
			break;
		case 2:
			createSchemaObject(
				ch, "TABLE", tableName, extra[0],
				"ENGINE = " + extra[1]
			);
			break;
		default:
			throw new IllegalArgumentException(
				"incorrect number of extra args"
			);
		}
	}

	protected void createTable(
		String tableName, String... extra
	) throws SQLException {
		createTable(channel(), tableName, extra);
	}

	protected void createView(
		String viewName, String viewDefn
	) throws SQLException {
		createSchemaObject(channel(), "VIEW", viewName, viewDefn);
	}

	protected void createProcedure(
		Channel ch, String procedureName, String procedureDefn
	) throws SQLException {
		createSchemaObject(
			ch, "PROCEDURE", procedureName, procedureDefn
		);
	}

	protected void createProcedure(
		String procedureName, String procedureDefn
	) throws SQLException {
		createProcedure(channel(), procedureName, procedureDefn);
	}

	protected class SchemaObject {
		SchemaObject(
			Channel ch_, String type_, String name_
		) throws SQLException {
			ch = ch_;
			type = type_;
			name = name_;

			try {
				drop();
			} catch (SQLException e) {
				if (!e.getMessage().startsWith(
					"Operation DROP USER failed"
				))
					throw e;
			}
		}

		void drop() throws SQLException {
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

		final Channel ch;
		final String type;
		final String name;
	}

	protected void createSchemaObject(
		Channel ch, String objectType, String objectName,
		String... extra
	) throws SQLException {
		createdObjects.offerLast(
			new SchemaObject(ch, objectType, objectName)
		);

		StringBuilder sb = new StringBuilder();
		sb.append("CREATE ").append(
			objectType
		).append(' ').append(objectName);

		for (String s: extra) {
			sb.append(' ').append(s);
		}

		String sql = sb.toString();

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

	@BeforeMethod
	public void setUp() {
	}

	@AfterMethod
	public void tearDown() throws Exception {
		for (
			SchemaObject s = createdObjects.pollLast();
			s != null;
			s = createdObjects.pollLast()
		)
			s.drop();

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
