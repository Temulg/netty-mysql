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
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.TypeId;
import udentric.mysql.classic.type.ValueAdapter;

public class AnyByteArray implements ValueAdapter<byte[]> {
	public AnyByteArray(TypeId id_) {
		id = id_;
	}

	@Override
	public TypeId typeId() {
		return id;
	}

	@Override
	public void encodeValue(
		ByteBuf dst, byte[] value, AdapterState state,
		int bufSoftLimit, FieldImpl fld
	) {
		Integer offset = state.get();
		if (offset == null) {
			Packet.writeIntLenenc(dst, value.length);
			if (value.length <= bufSoftLimit) {
				dst.writeBytes(value);
				state.markAsDone();
			} else {
				dst.writeBytes(value, 0, bufSoftLimit);
				offset = bufSoftLimit;
				state.set(offset);
			}
		} else {
			int rem = value.length - offset;
			if (rem <= bufSoftLimit) {
				dst.writeBytes(value, offset, rem);
				state.markAsDone();
			} else {
				dst.writeBytes(value, offset, bufSoftLimit);
				offset += bufSoftLimit;
				state.set(offset);
			}
		}
	}

	@Override
	public byte[] decodeValue(
		byte[] dst, ByteBuf src, AdapterState state,
		FieldImpl fld
	) {
		Integer offset = state.get();
		if (offset == null) {
			int sz = Packet.readIntLenencSafe(src);
			switch (sz) {
			case Packet.LENENC_INCOMPLETE:
				return null;
			case Packet.LENENC_NULL:
				state.markAsDone();
				return null;
			}

			if (dst == null) {
				dst = new byte[sz];
			} else if (dst.length < sz) {
				throw new IllegalStateException(String.format(
					"supplied byte array is too small: "
					+ "has %d, want %d", dst.length, sz
				));
			}

			if (src.readableBytes() >= sz) {
				src.readBytes(dst, 0, sz);
				state.markAsDone();
			} else {
				offset = src.readableBytes();
				src.readBytes(dst, 0, offset);
				state.set(offset);
			}
		} else {
			int rem = dst.length - offset;
			int readable = src.readableBytes();
			if (readable >= rem) {
				src.readBytes(dst, offset, rem);
				state.markAsDone();
			} else {
				src.readBytes(dst, offset, readable);
				offset += readable;
				state.set(offset);
			}
		}

		return dst;
	}

	private final TypeId id;
}
