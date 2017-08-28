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

package udentric.mysql;

import java.net.SocketAddress;
import java.util.Map;
import java.util.TimeZone;
import org.apache.logging.log4j.Logger;
import udentric.mysql.util.ExceptionInterceptor;

public interface Session {
	Config getConfig();

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

	Logger getLogger();

	void setLogger(Logger logger);

	void configureTimezone();

	TimeZone getDefaultTimeZone();

	String getErrorMessageEncoding();

	int getMaxBytesPerChar(String javaCharsetName);

	int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

	String getEncodingForIndex(int collationIndex);

	ServerSession getServerSession();

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
}
