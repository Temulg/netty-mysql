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

import com.google.common.base.MoreObjects;

import io.netty.buffer.ByteBuf;

public class ServerAck	{
	public static ServerAck fromOk(ByteBuf msg, SessionInfo si) {
		long rows = Packet.readLongLenenc(msg);
		long insertId = Packet.readLongLenenc(msg);
		short srvStatus = 0;
		short warnCount = 0;
		String info = "";
		SessionStateInfo stateInfo = SessionStateInfo.EMPTY;

		if (ClientCapability.PROTOCOL_41.get(si.clientCaps)) {
			srvStatus = msg.readShortLE();
			warnCount = msg.readShortLE();
		} else if (ClientCapability.TRANSACTIONS.get(si.clientCaps)) {
			srvStatus = msg.readShortLE();
		}

		if (ClientCapability.SESSION_TRACK.get(si.clientCaps)) {
			int sz = Packet.readIntLenenc(msg);
			if (sz > 0) {
				info = msg.readCharSequence(
					sz, si.charset()
				).toString();
			}

			if (ServerStatus.SESSION_STATE_CHANGED.get(
				srvStatus
			)) {
				stateInfo = new SessionStateInfo(
					msg, si.charset()
				);
			}
		} else {
			info = msg.readCharSequence(
				msg.readableBytes(), si.charset()
			).toString();
		}

		return new ServerAck(
			rows, insertId, srvStatus, warnCount, info, stateInfo
		);
	}

	private ServerAck(
		long affectedRows_, long insertId_, short srvStatus_,
		short warnCount_, String info_, SessionStateInfo stateInfo_
	) {
		affectedRows = affectedRows_;
		insertId = insertId_;
		srvStatus = srvStatus_;
		warnCount = warnCount_;
		info = info_;
		stateInfo = stateInfo_;
	}

	public static ServerAck fromEof(ByteBuf msg, SessionInfo si) {
		if (ClientCapability.PROTOCOL_41.get(si.clientCaps)) {
			return new ServerAck(
				msg.readShortLE(),
				msg.readShortLE()
			);
		} else {
			return new ServerAck((short)0, (short)0);
		}
	}

	private ServerAck(short warnCount_, short srvStatus_) {
		affectedRows = 0;
		insertId = 0;
		srvStatus = srvStatus_;
		warnCount = warnCount_;
		info = "";
		stateInfo = SessionStateInfo.EMPTY;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add(
			"affectedRows", affectedRows
		).add(
			"insertId", insertId
		).add(
			"srvStatus", Integer.toHexString(srvStatus)
		).add(
			"warnCount", warnCount
		).add(
			"info", info
		).add(
			"stateInfo", stateInfo
		).toString();
	}

	public final long affectedRows;
	public final long insertId;
	public final short srvStatus;
	public final short warnCount;
	public final String info;
	public final SessionStateInfo stateInfo;
}
