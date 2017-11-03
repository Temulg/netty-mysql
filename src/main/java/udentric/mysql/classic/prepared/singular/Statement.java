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

package udentric.mysql.classic.prepared.singular;

import java.util.BitSet;

import udentric.mysql.FieldSet;
import udentric.mysql.classic.FieldSetImpl;

class Statement implements udentric.mysql.classic.prepared.Statement {
	Statement(
		String sql_, int srvId_,
		FieldSetImpl parameters_,
		FieldSetImpl columns_
	) {
		sql = sql_;
		srvId = srvId_;
		parameters = parameters_;
		columns = columns_;
		parameterPreloaded = new BitSet(parameters.size());
	}

	@Override
	public boolean typesDeclared() {
		return typesDeclared;
	}

	@Override
	public void typesDeclared(boolean v) {
		typesDeclared = v;
	}

	@Override
	public void markParameterPreloaded(int pos) {
		parameterPreloaded.set(pos);
	}

	@Override
	public boolean parameterPreloaded(int pos) {
		return parameterPreloaded.get(pos);
	}

	@Override
	public void resetPreloaded() {
		parameterPreloaded.clear();
	}

	@Override
	public int getServerId() {
		return srvId;
	}

	@Override
	public FieldSet parameters() {
		return parameters;
	}

	@Override
	public FieldSet columns() {
		return columns;
	}

	final String sql;
	final int srvId;
	private final FieldSetImpl parameters;
	private final FieldSetImpl columns;
	private final BitSet parameterPreloaded;
	private boolean typesDeclared;
}
