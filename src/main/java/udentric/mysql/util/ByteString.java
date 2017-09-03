/*
 * Copyright (c) 2017 Alex Dubov <oakad@yahoo.com>
 *
 * This file is made available under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package udentric.mysql.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;

public class ByteString {
	public ByteString(String s) {
		this(s, StandardCharsets.UTF_8);
	}

	public ByteString(String s, Charset cs) {
		bytes = s.getBytes(cs);
		hashCode = Arrays.hashCode(bytes);
	}

	public ByteString(ByteBuf in, int length) {
		bytes = new byte[length];
		in.readBytes(bytes);
		hashCode = Arrays.hashCode(bytes);
	}

	@Override
	public String toString() {
		return new String(bytes, StandardCharsets.UTF_8);
	}

	public String toString(Charset cs) {
		return new String(bytes, cs);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public final boolean equals(Object other_) {
		if (this == other_)
			return true;

		if (other_ instanceof ByteString) {
			ByteString other = (ByteString)other_;

			return Arrays.equals(bytes, other.bytes);
		} else
			return false;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public int size() {
		return bytes.length;
	}

	private final byte[] bytes;
	private final int hashCode;
}
