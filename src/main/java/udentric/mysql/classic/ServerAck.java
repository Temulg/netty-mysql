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

import java.nio.charset.Charset;

import com.google.common.base.MoreObjects;

import io.netty.buffer.ByteBuf;

public class ServerAck	{
	public ServerAck(ByteBuf msg, boolean okPacket, Charset cs) {
		rows = okPacket ? Packet.readLongLenenc(msg) : 0;
		insertId = okPacket ? Packet.readLongLenenc(msg) : 0;
		srvStatus = msg.readShortLE();
		warnCount = Packet.readInt2(msg);

		info = okPacket ? msg.readCharSequence(
			msg.readableBytes(), cs
		).toString() : "";
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add(
			"rows", rows
		).add(
			"insertId", insertId
		).add(
			"srvStatus", srvStatus
		).add(
			"warnCount", warnCount
		).add(
			"info", info
		).toString();
	}

	public final long rows;
	public final long insertId;
	public final short srvStatus;
	public final int warnCount;
	public final String info;
}
