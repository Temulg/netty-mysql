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

package udentric.mysql.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import io.netty.buffer.ByteBuf;

public class Scramble411 {
	private Scramble411() {}

	@SuppressWarnings("deprecation")
	public static void encode(
		ByteBuf out, byte[] password, byte[] secret
	) {
		HashFunction hf = Hashing.sha1();

		byte[] s0 = hf.hashBytes(password).asBytes();
		byte[] s1 = hf.hashBytes(s0).asBytes();
		byte[] s2 = hf.newHasher().putBytes(
			secret
		).putBytes(s1).hash().asBytes();

		for (int pos = 0; pos < s0.length; pos++) {
			out.writeByte(s0[pos] ^ s2[pos]);
		}
	}
}
