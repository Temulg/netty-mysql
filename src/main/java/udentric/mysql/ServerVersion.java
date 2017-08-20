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

package udentric.mysql;

public class ServerVersion implements Comparable<ServerVersion> {
	public ServerVersion(String value_) {
		value = value_;
		parseVersionString();
	}

	public boolean meetsMinimum(int... version) {
		int ordPos = 0;

		for (int ord: version) {
			if (ord > ordinals[ordPos])
				return false;

			ordPos++;
			if (ordPos == ordinals.length)
				break;
		}
		return true;
	}

	@Override
	public String toString() {
		return value;
	}

	@Override
	public int compareTo(ServerVersion other) {
		int ordPos = 0;
		int rv = 0;

		for (int ord: ordinals) {
			rv = Integer.compare(ord, other.ordinals[ordPos]);
			if (rv != 0)
				break;
			ordPos++;
		}
		return rv;
	}

	private void parseVersionString() {
		int ordPos = 0;

		for (int vpos = 0; vpos < value.length(); vpos++) {
			char c = value.charAt(vpos);

			if (Character.isDigit(c)) {
				ordinals[ordPos] *= 10;
				ordinals[ordPos] += c - '0';
			} else if (c == '.') {
				ordPos++;
				if (ordPos == ordinals.length)
					return;
			} else
				return;
		}
	}

	private final String value;
	private final int[] ordinals = new int[3];
}
