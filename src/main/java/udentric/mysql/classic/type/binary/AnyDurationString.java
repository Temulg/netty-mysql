/*
 * Copyright (c) 2017 - 2018 Alex Dubov <oakad@yahoo.com>
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
import io.netty.buffer.ByteBufUtil;
import java.nio.CharBuffer;
import java.time.Duration;
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.TimeUtils;
import udentric.mysql.classic.type.ValueAdapter;
import udentric.mysql.classic.type.TypeId;

public class AnyDurationString implements ValueAdapter<Duration> {
        public AnyDurationString(TypeId id_) {
                id = id_;
        }
		@Override
	public TypeId typeId() {
		return id;
	}

	@Override
	public void encodeValue(
		ByteBuf dst, Duration value, AdapterState state,
		int bufSoftLimit, FieldImpl fld
	) {
		ByteBuf valBuf = ByteBufUtil.encodeString(
			state.alloc,
			CharBuffer.wrap(TimeUtils.formatString(value)),
			fld.encoding.charset
		);
		Packet.writeIntLenenc(dst, valBuf.readableBytes());
		dst.writeBytes(valBuf);
		valBuf.release();
		state.markAsDone();
	}

	@Override
	public Duration decodeValue(
		Duration dst, ByteBuf src, AdapterState state,
		FieldImpl fld
	) {
		Integer sz = state.get();

		if (sz == null) {
			sz = Packet.readIntLenencSafe(src);
			if (sz < 0)
				return null;
		}

		if (src.readableBytes() >= sz) {
			state.markAsDone();
			return TimeUtils.parseDuration(
				src.readCharSequence(sz, fld.encoding.charset)
			);	
		} else {
			state.set(sz);
			return null;
		}
	}

	private final TypeId id;
}
