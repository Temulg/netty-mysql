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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;

import java.util.Arrays;
import java.util.concurrent.locks.StampedLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import udentric.mysql.Config;
import udentric.mysql.ServerVersion;
import udentric.mysql.classic.dicta.Dictum;
import udentric.mysql.classic.dicta.InitDb;
import udentric.mysql.classic.dicta.MysqlNativePasswordAuth;
import udentric.mysql.classic.dicta.Query;
import udentric.mysql.classic.dicta.TextResultSet;
import udentric.mysql.classic.dicta.ResultSet;

public class Session {
	Session(Client cl_) {
		cl = cl_;
		serverCharset = CharsetInfo.forId(223);
	}

	public Config getConfig() {
		return cl.getConfig();
	}

	public ServerVersion getServerVersion() {
		return serverVersion;
	}

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalog_) {
		catalog = catalog_;
	}

	public ServerSession getServerSession() {
		return null;
	}

	public Query newQuery(String sql) {
		return new Query(sql, serverCharset);
	}

	public InitDb newInitDb(String catalog) {
		return new InitDb(catalog, serverCharset);
	}

	Throwable beginRequest(Dictum dct) {
		long stamp = lock.tryWriteLock();

		if (stamp == 0 || currentDictum != null) {
			System.err.format(
				"--6- current dictum %s, new dictum %s\n",
				currentDictum, dct
			);
			return new RuntimeException("channel busy");
		}

		currentDictum = dct;
		lock.unlockWrite(stamp);
		return null;
	}

	public void discardCommand() {
		long stamp = lock.writeLock();
		currentDictum = null;
		lock.unlockWrite(stamp);
	}

	public void discardCommand(Throwable cause) {
		Dictum dct = null;
		long stamp = lock.writeLock();
		dct = currentDictum;
		currentDictum = null;
		lock.unlockWrite(stamp);

		if (dct != null) {
			dct.handleFailure(cause);
		}
	}

	Dictum getCurrentCommand() {
		long stamp = lock.tryOptimisticRead();
		Dictum dct = currentDictum;

		if (!lock.validate(stamp)) {
			stamp = lock.readLock();
			dct = currentDictum;
			lock.unlockRead(stamp);
		}
		return dct;
	}

	public void receiveTextResultSet(
		int seqNum, int columnCount, ResponseConsumer rc,
		ChannelPromise chp
	) {
		long stamp = lock.writeLock();
		currentDictum = new TextResultSet(
			columnCount,
			!ClientCapability.DEPRECATE_EOF.get(clientCaps),
			seqNum, serverCharset
		).withChannelPromise(
			chp
		).withResponseConsumer(
			rc
		);
		lock.unlockWrite(stamp);
	}

	public Dictum handleInitialHandshake(
		ByteBuf msg, ChannelHandlerContext ctx
	) throws Exception {
		long stamp = lock.writeLock();
		currentDictum = null;

		try {
			int seqNum = Packet.getSeqNum(msg);
			msg.skipBytes(Packet.HEADER_SIZE);

			int protoVers = Packet.readInt1(msg);
			if (PROTOCOL_VERSION != protoVers) {
				throw new DecoderException(
					"unsupported protocol version "
					+ Integer.toString(protoVers)
				);
			}

			String authPluginName
			= MysqlNativePasswordAuth.AUTH_PLUGIN_NAME;

			serverVersion = new ServerVersion(Packet.readStringNT(
				msg, serverCharset.javaCharset
			));
			srvConnId = msg.readIntLE();
			LOGGER.debug(
				"server identity set: {} ({})", srvConnId,
				serverVersion
			);

			byte[] secret = new byte[8];
			msg.readBytes(secret);
			msg.skipBytes(1);

			if (!msg.isReadable()) {
				return selectAuthCommand(
					authPluginName, secret, seqNum, ctx
				);
			}

			serverCaps = Packet.readLong2(msg);

			if (!msg.isReadable()) {
				updateClientCapabilities();
				return selectAuthCommand(
					authPluginName, secret, seqNum, ctx
				);
			}

			serverCharset = CharsetInfo.forId(
				Packet.readInt1(msg)
			);

			msg.skipBytes(2);//short statusFlags = msg.readShortLE();

			serverCaps |= Packet.readLong2(msg) << 16;
			updateClientCapabilities();
			int s2Len = Packet.readInt1(msg);
			msg.skipBytes(10);

			if (ClientCapability.PLUGIN_AUTH.get(serverCaps)) {
				int oldLen = secret.length;
				secret = Arrays.copyOf(secret, s2Len - 1);
				msg.readBytes(
					secret, oldLen, s2Len - oldLen - 1
				);
				msg.skipBytes(1);
				authPluginName = Packet.readStringNT(
					msg, serverCharset.javaCharset
				);
			} else {
				s2Len = Math.max(12, msg.readableBytes());
				int oldLen = secret.length;
				secret = Arrays.copyOf(secret, oldLen + s2Len);
				msg.readBytes(secret, oldLen, s2Len);
			}

			return selectAuthCommand(
				authPluginName, secret, seqNum, ctx
			);
		} finally {
			lock.unlockWrite(stamp);
		}
	}

	private Dictum selectAuthCommand(
		String authPluginName, byte[] secret, int seqNum,
		ChannelHandlerContext ctx
	) throws Exception {
		ByteBuf attrBuf = null;

		switch (authPluginName) {
		case "mysql_native_password":
			if (ClientCapability.CONNECT_ATTRS.get(clientCaps)) {
				attrBuf = encodeAttrs(ctx);
			}

			if (getConfig().containsKey(Config.Key.password)) {
				clientCaps |= ClientCapability.SECURE_CONNECTION.mask();
				if (ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.get(
					serverCaps
				))
					clientCaps
					|= ClientCapability.PLUGIN_AUTH_LENENC_CLIENT_DATA.mask();
			}

			++seqNum;
			return new MysqlNativePasswordAuth(
				seqNum, clientCaps, secret,
				getConfig().getOrDefault(
					Config.Key.maxPacketSize, 0xffffff
				), serverCharset, attrBuf
			);
		default:
			throw new IllegalStateException("unsupported auth");
		}
	}

	private ByteBuf encodeAttrs(ChannelHandlerContext ctx) {
		ByteBuf buf = ctx.alloc().buffer();
		cl.connAttributes.forEach((k, v) -> {
			byte[] b = k.getBytes(serverCharset.javaCharset);
			Packet.writeIntLenenc(buf, b.length);
			buf.writeBytes(b);
			b = v.getBytes(serverCharset.javaCharset);
			Packet.writeIntLenenc(buf, b.length);
			buf.writeBytes(b);
		});
		return buf;
	}

	private void updateClientCapabilities() {
		clientCaps = ClientCapability.LONG_PASSWORD.mask()
			| ClientCapability.LONG_FLAG.mask()
			| ClientCapability.PROTOCOL_41.mask()
			| ClientCapability.TRANSACTIONS.mask()
			| ClientCapability.MULTI_STATEMENTS.mask()
			| ClientCapability.MULTI_RESULTS.mask()
			| ClientCapability.PS_MULTI_RESULTS.mask()
			| ClientCapability.PLUGIN_AUTH.mask();

		if (ClientCapability.CONNECT_ATTRS.get(serverCaps))
			clientCaps |= ClientCapability.CONNECT_ATTRS.mask();
	}

	/*
	void changeUser(String userName, String password, String database);

	ExceptionInterceptor getExceptionInterceptor();

	void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor);

	boolean inTransactionOnServer();

	String getServerVariable(String name);

	int getServerVariable(String variableName, int fallbackValue);

	Map<String, String> getServerVariables();

	void abortInternal();

	void quit();

	void forceClose();

	boolean versionMeetsMinimum(int major, int minor, int subminor);

	long getThreadId();

	boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag);

	void setLogger(Logger logger);

	void configureTimezone();

	TimeZone getDefaultTimeZone();

	String getErrorMessageEncoding();

	int getMaxBytesPerChar(String javaCharsetName);

	int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

	String getEncodingForIndex(int collationIndex);


	boolean isSSLEstablished();

	SocketAddress getRemoteSocketAddress();

	boolean serverSupportsFracSecs();

	String getProcessHost();

	void addListener(SessionEventListener l);

	void removeListener(SessionEventListener l);

	public static interface SessionEventListener {
		void handleNormalClose();
		void handleReconnect();
		void handleCleanup(Throwable whyCleanedUp);
	}
	*/

	public final int PROTOCOL_VERSION = 10;
	public static final Logger LOGGER = LogManager.getLogger(Session.class);

	public enum State {
		IDLE;
	}

	private final Client cl;
	private final StampedLock lock = new StampedLock();
	private volatile Dictum currentDictum;
	private volatile ServerVersion serverVersion;
	private volatile CharsetInfo.Entry serverCharset;
	private volatile String catalog;
	private long serverCaps;
	private long clientCaps;
	private int srvConnId;
}
