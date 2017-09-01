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
import io.netty.handler.codec.DecoderException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import udentric.mysql.Config;
import udentric.mysql.classic.auth.NativePasswordCredentialsProvider;
import udentric.mysql.classic.command.Any;
import udentric.mysql.util.ByteString;

public class Session {
	Session(Client cl_) {
		cl = cl_;
	}

	public Config getConfig() {
		return cl.getConfig();
	}

	public ServerSession getServerSession() {
		return null;
	}

	Throwable beginRequest(Any cmd) {
		long stamp = lock.tryWriteLock();

		if (stamp == 0 || currentCommand != null) {
			return new RuntimeException("channel busy");
		}

		currentCommand = cmd;
		seqNum = 0;
		lock.unlock(stamp);
		return null;
	}

	int getSeqNum() {
		return seqNum;
	}

	void processClientFailure(Throwable cause) {
		long stamp = lock.writeLock();
		Any cmd = currentCommand;
		currentCommand = null;
		lock.unlock(stamp);
		cmd.handleFailure(cause);
	}

	boolean processServerMessage(ChannelHandlerContext ctx, ByteBuf msg) {
		Any cmd = currentCommand.get();
		if (cmd == null) {
			return false;
		}

		try {
			seqNum = Packet.getSeqNum(msg);
			msg.skipBytes(Packet.HEADER_SIZE);
			cmd.handleReply(msg, this);
		} finally {
			int remaining = msg.readableBytes();
			if (remaining > 0) {
				LOGGER.warn(
					"{} bytes left in incoming packet",
					remaining
				);
			}
			msg.release();

			return true;
		}
	}

	public void handleInitialHandshake(ByteBuf msg) {
		int protoVers = Fields.readInt1(msg);
		if (PROTOCOL_VERSION != protoVers) {
			processClientFailure(new DecoderException(
				"unsupported protocol version "
				+ Integer.toString(protoVers)
			));
			return;
		}

		ByteString authPluginName
		= NativePasswordCredentialsProvider.AUTH_PLUGIN_NAME;

		ph.updateServerIdentity(
			Fields.readStringNT(msg), msg.readIntLE()
		);

		byte[] secret = Fields.readBytes(msg, 8);
		msg.skipBytes(1);

		long srvCaps = 0;

		if (!msg.isReadable()) {
			reply(ph, authPluginName, secret);
			return;
		}

		srvCaps = Fields.readLong2(msg);

		if (!msg.isReadable()) {
			ph.updateServerCapabilities(srvCaps);
			reply(ph, authPluginName, secret);
			return;
		}

		ph.updateServerCharsetId(Fields.readInt1(msg));

		short statusFlags = msg.readShortLE();

		srvCaps |= Fields.readLong2(msg) << 16;

		int s2Len = Fields.readInt1(msg);

		ph.updateServerCapabilities(srvCaps);

		msg.skipBytes(10);

		if (ClientCapability.PLUGIN_AUTH.get(srvCaps)) {
			int oldLen = secret.length;
			secret = Arrays.copyOf(secret, s2Len - 1);
			msg.readBytes(secret, oldLen, s2Len - oldLen - 1);
			msg.skipBytes(1);
			authPluginName = Fields.readStringNT(msg);
		} else {
			s2Len = Math.max(12, msg.readableBytes());
			int oldLen = secret.length;
			secret = Arrays.copyOf(secret, oldLen + s2Len);
			msg.readBytes(secret, oldLen, s2Len);
		}

		reply(ph, authPluginName, secret);
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

	ServerVersion getServerVersion();

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
	private Any currentCommand;
	private int seqNum;
}
