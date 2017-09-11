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

package udentric.mysql.exceptions;

import java.net.BindException;
import udentric.mysql.Config;
import udentric.mysql.Messages;
import udentric.mysql.classic.ServerSession;

public class ExceptionFactory {
	public static CJException createException(String message) {
		return createException(CJException.class, message);
	}

	@SuppressWarnings("unchecked")
	public static <T extends CJException> T createException(
		Class<T> clazz, String message
	) {
		T sqlEx;
		try {
			sqlEx = clazz.getConstructor(
				String.class
			).newInstance(message);
		} catch (Throwable e) {
			sqlEx = (T)new CJException(message);
		}
		return sqlEx;
	}

	public static CJException createException(
		String message, Throwable cause
	) {
		return createException(CJException.class, message, cause);
	}

	public static <T extends CJException> T createException(
		Class<T> clazz, String message, Throwable cause
	) {
		T sqlEx = createException(clazz, message);

		if (cause != null) {
			try {
				sqlEx.initCause(cause);
			} catch (Throwable t) {}

			if (cause instanceof CJException) {
				sqlEx.setSQLState(
					((CJException)cause).getSQLState()
				);
				sqlEx.setVendorCode(
					((CJException)cause).getVendorCode()
				);
				sqlEx.setTransient(
					((CJException) cause).isTransient()
				);
			}
		}
		return sqlEx;
	}

	public static CJException createException(
		String message, String sqlState, int vendorErrorCode,
		boolean isTransient, Throwable cause
	) {
		CJException ex = createException(
			CJException.class, message, cause
		);
		ex.setSQLState(sqlState);
		ex.setVendorCode(vendorErrorCode);
		ex.setTransient(isTransient);
		return ex;
	}

	public static CJCommunicationsException createCommunicationsException(
		Config cfg, ServerSession serverSession,
		long lastPacketSentTimeMs, long lastPacketReceivedTimeMs,
		Throwable cause
	) {
		CJCommunicationsException sqlEx = createException(
			CJCommunicationsException.class, null, cause
		);
		sqlEx.init(
			cfg, serverSession, lastPacketSentTimeMs,
			lastPacketReceivedTimeMs
		);

		return sqlEx;
	}

	public static String createLinkFailureMessageBasedOnHeuristics(
		Config cfg, ServerSession serverSession,
		long lastPacketSentTimeMs,
		long lastPacketReceivedTimeMs, Throwable underlyingException
	) {
		long serverTimeoutSeconds = 0;
		boolean isInteractiveClient = cfg.getOrDefault(
			Config.Key.interactiveClient, false
		);

		String serverTimeoutSecondsStr = null;

		if (serverSession != null) {
			if (isInteractiveClient) {
				serverTimeoutSecondsStr = serverSession.getServerVariable(
					"interactive_timeout"
				);
			} else {
				serverTimeoutSecondsStr = serverSession.getServerVariable(
					"wait_timeout"
				);
			}
		}

		if (serverTimeoutSecondsStr != null) {
			try {
				serverTimeoutSeconds = Long.parseLong(
					serverTimeoutSecondsStr
				);
			} catch (NumberFormatException e) {
				serverTimeoutSeconds = 0;
			}
		}

		StringBuilder exceptionMessageBuf = new StringBuilder();
		long nowMs = System.currentTimeMillis();

		if (lastPacketSentTimeMs == 0) {
			lastPacketSentTimeMs = nowMs;
		}

		long timeSinceLastPacketSentMs = (nowMs - lastPacketSentTimeMs);
		long timeSinceLastPacketSeconds = timeSinceLastPacketSentMs / 1000;
		long timeSinceLastPacketReceivedMs = (nowMs - lastPacketReceivedTimeMs);

		int dueToTimeout = DUE_TO_TIMEOUT_FALSE;

		StringBuilder timeoutMessageBuf = null;

		if (serverTimeoutSeconds != 0) {
			if (timeSinceLastPacketSeconds > serverTimeoutSeconds) {
				dueToTimeout = DUE_TO_TIMEOUT_TRUE;
				timeoutMessageBuf = new StringBuilder();
				timeoutMessageBuf.append(Messages.getString(
					"CommunicationsException.2"
				)).append(isInteractiveClient ? Messages.getString(
						"CommunicationsException.4"
				) : Messages.getString(
					"CommunicationsException.3"
				));

			}
		} else if (
			timeSinceLastPacketSeconds > DEFAULT_WAIT_TIMEOUT_SECONDS
		) {
			dueToTimeout = DUE_TO_TIMEOUT_MAYBE;
			timeoutMessageBuf = new StringBuilder();
			timeoutMessageBuf.append(Messages.getString(
				"CommunicationsException.5"
			)).append(Messages.getString(
				"CommunicationsException.6"
			)).append(Messages.getString(
				"CommunicationsException.7"
			)).append(Messages.getString(
				"CommunicationsException.8"
			));
		}

		if (
			dueToTimeout == DUE_TO_TIMEOUT_TRUE
			|| dueToTimeout == DUE_TO_TIMEOUT_MAYBE
		) {
			if (lastPacketReceivedTimeMs != 0) {
				exceptionMessageBuf.append(Messages.getString(
					"CommunicationsException.ServerPacketTimingInfo",
					timeSinceLastPacketReceivedMs,
					timeSinceLastPacketSentMs
				));
			} else {
				exceptionMessageBuf.append(Messages.getString(
					"CommunicationsException.ServerPacketTimingInfoNoRecv", timeSinceLastPacketSentMs
				));
			}

			if (timeoutMessageBuf != null) {
				exceptionMessageBuf.append(timeoutMessageBuf);
			}

			exceptionMessageBuf.append(Messages.getString(
				"CommunicationsException.11"
			));
			exceptionMessageBuf.append(Messages.getString(
				"CommunicationsException.12"
			));
			exceptionMessageBuf.append(Messages.getString(
				"CommunicationsException.13"
			));
		} else {
			if (underlyingException instanceof BindException) {
				String localSocketAddress = cfg.getOrDefault(
					Config.Key.localSocketAddress, ""
				);

				if (
					!localSocketAddress.isEmpty()
					&& !interfaceExists(localSocketAddress)
				) {
					exceptionMessageBuf.append(Messages.getString(
						"CommunicationsException.LocalSocketAddressNotAvailable"
					));
				} else {
					exceptionMessageBuf.append(Messages.getString(
						"CommunicationsException.TooManyClientConnections"
					));
				}
			}
		}

		if (exceptionMessageBuf.length() == 0) {
			exceptionMessageBuf.append(Messages.getString(
				"CommunicationsException.20"
			));

			if (cfg.getOrDefault(
				Config.Key.maintainTimeStats, false
			) && !cfg.getOrDefault(
				Config.Key.paranoid, false
			)) {
				exceptionMessageBuf.append("\n\n");
				if (lastPacketReceivedTimeMs != 0) {
					exceptionMessageBuf.append(Messages.getString(
						"CommunicationsException.ServerPacketTimingInfo",
						timeSinceLastPacketReceivedMs, timeSinceLastPacketSentMs
					));
				} else {
					exceptionMessageBuf.append(Messages.getString(
						"CommunicationsException.ServerPacketTimingInfoNoRecv",
						timeSinceLastPacketSentMs
					));
				}
			}
		}

		return exceptionMessageBuf.toString();
	}

	public static boolean interfaceExists(String hostname) {
		try {
			Class<?> networkInterfaceClass = Class.forName(
				"java.net.NetworkInterface"
			);
			return networkInterfaceClass.getMethod(
				"getByName"
			).invoke(
				networkInterfaceClass, hostname
			) != null;
		} catch (Throwable t) {
			return false;
		}
	}

	private static final long DEFAULT_WAIT_TIMEOUT_SECONDS = 28800;
	private static final int DUE_TO_TIMEOUT_FALSE = 0;
	private static final int DUE_TO_TIMEOUT_MAYBE = 2;
	private static final int DUE_TO_TIMEOUT_TRUE = 1;
}
