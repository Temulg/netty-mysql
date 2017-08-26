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

import java.util.Optional;

import io.netty.buffer.ByteBuf;
import udentric.mysql.util.ByteString;

public class MySQLException extends RuntimeException {
	public static MySQLException fromErrPacket(ByteBuf in) {
		int code = Fields.readInt2(in);
		if (code == 0xffff) {
			int stage = Fields.readInt1(in);
			int max_stage = Fields.readInt1(in);
			int progress = 	in.readMediumLE();
			ByteString info = Fields.readStringLenenc(in);
			return new MySQLException(String.format(
				"[progress] %d, stage %d of %d, info: %s",
				progress, stage, max_stage, info
			));
		}

		Optional<String> state = Optional.empty();
		if (in.getByte(in.readerIndex()) == '#') {
			in.skipBytes(1);
			state = Optional.ofNullable(
				new ByteString(in, 5).toString()
			);
		}

		return new MySQLException(code, state, Optional.ofNullable(
			new ByteString(in, in.readableBytes()).toString()
		));
	}

	private MySQLException(String message) {
		super(message);
		code = 0xffff;
		state = Optional.empty();
		info = Optional.empty();
	}

	private MySQLException(
		int code_, Optional<String> state_, Optional<String> info_
	) {
		super(
			state_ != null
			? String.format("[%d] (%s) %s", code_, state_, info_)
			: String.format("[%d] %s", code_, info_)
		);
		code = code_;
		state = state_;
		info = info_;
	}

	static final long serialVersionUID = 0x55321f0dc6999cb7L;

	public final int code;
	public final Optional<String> state;
	public final Optional<String> info;
}
