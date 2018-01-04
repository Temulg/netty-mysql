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
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.TypeId;
import udentric.mysql.classic.type.ValueAdapter;

public class AnyShortLPByteArray implements ValueAdapter<byte[]> {
	public AnyShortLPByteArray(TypeId id_) {
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
		if (value.length > 255)
			throw new IllegalArgumentException("array is too big");

		dst.writeByte(value.length);
		dst.writeBytes(value);
		state.markAsDone();
	}

	@Override
	public byte[] decodeValue(
		byte[] dst, ByteBuf src, AdapterState state,
		FieldImpl fld
	) {
		Integer sz = state.get();
		if (sz == null)
			sz = Packet.readInt1(src);

		if (src.readableBytes() >= sz) {
			byte[] valBuf = new byte[sz];
			src.readBytes(valBuf);
			state.markAsDone();
			return valBuf;
		} else {
			state.set(sz);
			return null;
		}
	}

	private final TypeId id;
}
