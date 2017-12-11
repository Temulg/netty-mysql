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

package udentric.mysql.classic.type.binary;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.TypeId;
import udentric.mysql.classic.type.ValueAdapter;

class AnyNioWriteChannel implements ValueAdapter<GatheringByteChannel> {
	AnyNioWriteChannel(TypeId id_) {
		id = id_;
	}

	@Override
	public TypeId typeId() {
		return id;
	}

	@Override
	public void encodeValue(
		ByteBuf dst, GatheringByteChannel value, AdapterState state,
		int bufSoftLimit, FieldImpl fld
	) {
		throw new IllegalStateException("channel is not readable");
	}

	@Override
	public GatheringByteChannel decodeValue(
		GatheringByteChannel dst, ByteBuf src, AdapterState state,
		FieldImpl fld
	) {
		if (dst == null) {
			throw new IllegalStateException(
				"suitable Channel object must be supplied"
			);
		}

		Integer remaining = state.get();
		if (remaining == null) {
			remaining = Packet.readIntLenencSafe(src);
			if (remaining < 0)
				return dst;
		}

		int count = remaining;
		if (src.readableBytes() < count)
			count = src.readableBytes();

		try {
			src.readBytes(dst, count);
		} catch (IOException e) {
			Channels.throwAny(e);
		}
		remaining -= count;
		if (remaining == 0)
			state.markAsDone();
		else
			state.set(remaining);
		
		return dst;
	}

	private final TypeId id;
}
