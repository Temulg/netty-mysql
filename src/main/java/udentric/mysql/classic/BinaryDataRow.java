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

import io.netty.buffer.ByteBuf;
import udentric.mysql.DataRow;

public class BinaryDataRow implements DataRow {
	public BinaryDataRow(FieldSetImpl columns_, ByteBuf data_) {
		columns = columns_;
		index = new int[columns.size() * 2];
		data = data_;

		int offset = 0;

		for (int pos = 0; pos < columns.size(); pos++) {
			((FieldImpl)(columns.get(pos))).binaryValueByteSize(
				data, offset
			);
			offset = index[pos * 2] + index[pos * 2 + 1];
		}
	}

	@Override
	public void close() {
		data.release();
	}

	@Override
	public <T> T getValue(int pos, Class<T> cls) {
		return ((FieldImpl)(columns.get(pos))).binaryValueDecode(
			data, index[pos * 2], index[pos * 2 + 1], cls
		);
	}

	private void indexFields(int last) {
		int offset = 0;

		for (int pos = 0; pos <= last * 2; pos += 2) {
			if (index[pos] == 0) {
				
			}
			offset = index[pos] + index[pos + 1];
		}
	}

	private final FieldSetImpl columns;
	private final int[] index;		
	private final ByteBuf data;
}
