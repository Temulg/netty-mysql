/*
 * Copyright (c) 2017, 2018 Alex Dubov <oakad@yahoo.com>
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
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;

public class SessionStateInfo {
	public SessionStateInfo(ByteBuf msg, Charset cs) {
		ImmutableList.Builder<
			Entry
		> builder = ImmutableList.<Entry>builder();

		while (msg.readableBytes() > 0) {
			builder.add(new Entry(msg, cs));
		}
		entries = builder.build();
	}

	private SessionStateInfo() {
		entries = ImmutableList.of();
	}

	@Override
	public String toString() {
		ToStringHelper h = MoreObjects.toStringHelper(this);
		entries.forEach(entry -> {
			h.add(entry.type.name(), entry.info);
		});
		return h.toString();
	}

	public enum Type {
		SYSTEM_VARIABLES,
		SCHEMA,
		STATE_CHANGE,
		GTIDS,
		TRANSACTION_CHARACTERISTICS,
		TRANSACTION_STATE;
	};

	private static final Type[] types = Type.values();

	public static final SessionStateInfo EMPTY = new SessionStateInfo();

	public static class Entry {
		private Entry(ByteBuf msg, Charset cs) {
			type = types[Packet.readInt1(msg)];
			int sz = Packet.readIntLenenc(msg);
			info = msg.readCharSequence(sz, cs).toString();
		}

		public final Type type;
		public final String info;
	}

	public final ImmutableList<Entry> entries;
}
