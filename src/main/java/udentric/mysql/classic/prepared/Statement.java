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

package udentric.mysql.classic.prepared;

import io.netty.channel.Channel;
import java.util.BitSet;

import udentric.mysql.FieldSet;
import udentric.mysql.classic.FieldSetImpl;

public class Statement implements udentric.mysql.PreparedStatement {
	public Statement(
		String sql_, int srvId_, Channel ch_,
		FieldSetImpl parameters_,
		FieldSetImpl columns_
	) {
		sql = sql_;
		srvId = srvId_;
		ch = ch_;
		parameters = parameters_;
		columns = columns_;
		parameterPreloaded = new BitSet(parameters.size());
	}

	public void check(Channel ch_) {
		if (ch != ch_)
			throw new IllegalStateException(
				"statement was prepared for different channel"
			);

		if (discarded)
			throw new IllegalStateException(
				"statement is unusable"
			);
	}

	public void discard() {
		discarded = true;
	}

	public boolean typesDeclared() {
		return typesDeclared;
	}

	public void typesDeclared(boolean v) {
		typesDeclared = v;
	}

	public void markParameterPreloaded(int pos) {
		parameterPreloaded.set(pos);
	}

	public boolean parameterPreloaded(int pos) {
		return parameterPreloaded.get(pos);
	}

	public void resetPreloaded() {
		parameterPreloaded.clear();
	}

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

	private final String sql;
	private final int srvId;
	private final Channel ch;
	private final FieldSetImpl parameters;
	private final FieldSetImpl columns;
	private final BitSet parameterPreloaded;
	private boolean typesDeclared;
	private boolean discarded;
}
