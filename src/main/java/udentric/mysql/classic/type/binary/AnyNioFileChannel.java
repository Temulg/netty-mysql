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
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;

import udentric.mysql.classic.Channels;
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.TypeId;
import udentric.mysql.classic.type.ValueAdapter;

public class AnyNioFileChannel implements ValueAdapter<FileChannel> {
	public AnyNioFileChannel(TypeId id_) {
		id = id_;
	}

	@Override
	public TypeId typeId() {
		return id;
	}

	@Override
	public void encodeValue(
		ByteBuf dst, FileChannel value, AdapterState state,
		int bufSoftLimit, FieldImpl fld
	) {
		try {
			Long remaining = state.get();
			if (remaining == null) {
				long sz = value.size();
				Packet.writeLongLenenc(dst, sz);
				if (sz == 0) {
					state.markAsDone();
					return;
				}

				long lim = Math.min(sz, bufSoftLimit);
				long wc = dst.writeBytes(value, (int)lim);
				if (wc == sz) {
					state.markAsDone();
					return;
				} else if (wc < 0) {
					throw new IOException("file truncated");
				}

				state.set(sz - wc);
			} else {
				long lim = Math.min(remaining, bufSoftLimit);
				long wc = dst.writeBytes(value, (int)lim);
				if (wc == remaining) {
					state.markAsDone();
					return;
				} else if (wc < 0) {
					throw new IOException("file truncated");
				}

				state.set(remaining - wc);
			}
		} catch (IOException e) {
			Channels.throwAny(e);
		}
	}

	@Override
	public FileChannel decodeValue(
		FileChannel dst, ByteBuf src, AdapterState state,
		FieldImpl fld
	) {
		throw new IllegalStateException("channel is not writable");
	}

	private final TypeId id;
}
