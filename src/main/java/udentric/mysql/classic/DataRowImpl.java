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

import java.util.Arrays;
import udentric.mysql.DataRow;
import udentric.mysql.classic.type.ValueAdapter;

public class DataRowImpl implements DataRow {
	public DataRowImpl(
		FieldSetImpl columns, ColumnValueMapper mapper, boolean binary
	) {
		int colCount = columns.size();
		colTypes = new Class[colCount];
		colValues = new Object[colCount];
		adapters = new ValueAdapter[colCount];
		nullBitmap = new byte[(colCount + 9) / 8];

		mapper.initRowTypes(colTypes);
		if (binary)
			initAdaptersBinary(columns, mapper);
		else
			initAdaptersText(columns, mapper);
	}

	private final void initAdaptersText(
		FieldSetImpl columns, ColumnValueMapper mapper
	) {
		for (int pos = 0; pos < adapters.length; pos++) {
			FieldImpl fld = (FieldImpl)columns.get(pos);
			if (colTypes[pos] != null)
				adapters[pos] = fld.type.textAdapterSelector.find(
					colTypes[pos]
				);
			else
				adapters[pos] = fld.type.textAdapterSelector.get();
		}
	}

	private final void initAdaptersBinary(
		FieldSetImpl columns, ColumnValueMapper mapper
	) {
		for (int pos = 0; pos < adapters.length; pos++) {
			FieldImpl fld = (FieldImpl)columns.get(pos);
			if (colTypes[pos] != null)
				adapters[pos] = fld.type.binaryAdapterSelector.find(
					colTypes[pos]
				);
			else
				adapters[pos] = fld.type.binaryAdapterSelector.get();
		}
	}

	@Override
	public int size() {
		return colValues.length;
	}

	@Override
	public <T> T getValue(int pos) {
		return (T)colValues[pos];
	}

	public void resetData(ColumnValueMapper mapper) {
		Arrays.fill(colValues, null);
		mapper.initRowValues(colValues);
		colPos = 0;
	}

	private final Class[] colTypes;
	private final Object[] colValues;
	private final ValueAdapter[] adapters;
	private final byte[] nullBitmap;
	int colPos;
}
