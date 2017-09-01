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

import java.util.Map;
import udentric.mysql.ServerCapabilities;
import udentric.mysql.ServerVersion;

public interface ServerSession {
	ServerCapabilities getCapabilities();

	void setCapabilities(ServerCapabilities capabilities);

	int getStatusFlags();

	void setStatusFlags(int statusFlags);

	void setStatusFlags(int statusFlags, boolean saveOldStatusFlags);

	int getOldStatusFlags();

	void setOldStatusFlags(int statusFlags);

	int getServerDefaultCollationIndex();

	void setServerDefaultCollationIndex(int serverDefaultCollationIndex);

	int getTransactionState();

	boolean inTransactionOnServer();

	boolean cursorExists();

	boolean isAutocommit();

	boolean hasMoreResults();

	boolean isLastRowSent();

	boolean noGoodIndexUsed();

	boolean noIndexUsed();

	boolean queryWasSlow();

	long getClientParam();

	void setClientParam(long clientParam);

	boolean useMultiResults();

	boolean hasLongColumnInfo();

	void setHasLongColumnInfo(boolean hasLongColumnInfo);

	Map<String, String> getServerVariables();

	String getServerVariable(String name);

	int getServerVariable(String variableName, int fallbackValue);

	void setServerVariables(Map<String, String> serverVariables);

	ServerVersion getServerVersion();

	boolean isVersion(ServerVersion version);

	String getServerDefaultCharset();

	String getErrorMessageEncoding();

	void setErrorMessageEncoding(String errorMessageEncoding);

	int getMaxBytesPerChar(String javaCharsetName);

	int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

	String getEncodingForIndex(int collationIndex);

	void configureCharacterSets();

	String getCharacterSetMetadata();

	void setCharacterSetMetadata(String characterSetMetadata);

	int getMetadataCollationIndex();

	void setMetadataCollationIndex(int metadataCollationIndex);

	String getCharacterSetResultsOnServer();

	void setCharacterSetResultsOnServer(String characterSetResultsOnServer);

	boolean isLowerCaseTableNames();

	boolean storesLowerCaseTableNames();

	public static int TRANSACTION_NOT_STARTED = 0;
	public static int TRANSACTION_IN_PROGRESS = 1;
	public static int TRANSACTION_STARTED = 2;
	public static int TRANSACTION_COMPLETED = 3;

	public static final String LOCAL_CHARACTER_SET_RESULTS = "local.character_set_results";
}
