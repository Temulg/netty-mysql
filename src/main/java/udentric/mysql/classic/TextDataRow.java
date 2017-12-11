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
import java.util.Arrays;
import udentric.mysql.DataRow;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.ValueAdapter;

public class TextDataRow implements DataRow {
	@SuppressWarnings("unchecked")
	public static TextDataRow init(
		TextDataRow current, FieldSetImpl columns,
		ColumnValueMapper mapper
	) {
		if (current == null || current.size() != columns.size())
			current = new TextDataRow(columns.size());
		else {
			Arrays.fill(current.colTypes, null);
			current.reset();
		}

		mapper.initRowTypes(current.colTypes);

		for (int pos = 0; pos < current.adapters.length; pos++) {
			FieldImpl fld = (FieldImpl)columns.get(pos);
			if (current.colTypes[pos] != null)
				current.adapters[pos] = fld.type.textAdapterSelector.find(
					current.colTypes[pos]
				);
			else
				current.adapters[pos] = fld.type.textAdapterSelector.get();
		}

		return current;
	}

	private TextDataRow(int colCount) {
		colTypes = new Class[colCount];
		colValues = new Object[colCount];
		adapters = new ValueAdapter[colCount];
	}

	public void reset() {
		Arrays.fill(colValues, null);
		Arrays.fill(adapters, null);
	}

	public void reset(ColumnValueMapper mapper) {
		reset();
		mapper.initRowValues(colValues);
	}

	@SuppressWarnings("unchecked")
	public void decodeValue(
		int col, ByteBuf src, AdapterState state, FieldSetImpl columns
	) {
		ValueAdapter a = adapters[col];

		colValues[col] = a.decodeValue(
			colValues[col], src, state, (FieldImpl)columns.get(col)
		);
	}

	@Override
	public int size() {
		return colValues.length;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getValue(int pos) {
		return (T)colValues[pos];
	}

	private final Class[] colTypes;
	private final Object[] colValues;
	private final ValueAdapter[] adapters;
}
