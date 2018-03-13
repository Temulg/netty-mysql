/*
 * Copyright (c) 2018 Alex Dubov <oakad@yahoo.com>
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
package udentric.mysql;

public class SqlException extends RuntimeException {
	public SqlException(String reason_, String state_, int code_) {
		super(String.format(
			"server error: [%s] %s (%d)",
			state_, reason_, code_
		)); 
		reason = reason_;
		state = state_;
		code = code_;
	}

	public SqlException(String reason_, String state_) {
		super(String.format(
			"server error: [%s] %s",
			state_, reason_
		)); 
		reason = reason_;
		state = state_;
		code = 0;
	}

	static final long serialVersionUID = 0xa4a2f866d25e465aL;

	@SuppressWarnings("unused")
	private final String reason;

	@SuppressWarnings("unused")
	private final String state;

	@SuppressWarnings("unused")
	private final int code;
}
