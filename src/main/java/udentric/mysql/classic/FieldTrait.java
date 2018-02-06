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

package udentric.mysql.classic;

import udentric.mysql.util.BitsetEnum;

public enum FieldTrait implements BitsetEnum<Short> {
	NOT_NULL(0, "field can't be NULL"),
	PRIMARY_KEY(1, "field is part of a primary key"),
	UNIQUE_KEY(2, "field is part of a unique key"),
	MULTIPLE_KEY(3, "field is part of a key"),
	BLOB(4, "field is a blob"),
	UNSIGNED(5, "field is unsigned"),
	ZEROFILL(6, "field is zerofill"),
	BINARY(7, "field is binary"),
	ENUM(8, "field is an enum"),
	AUTO_INCREMENT(9, "field is a autoincrement field"),
	TIMESTAMP(10, "field is a timestamp"),
	SET(11, "field is a set"),
	NO_DEFAULT_VALUE(12, "field doesn't have default value"),
	ON_UPDATE_NOW(13, "field is set to NOW on UPDATE"),
	NUM(15, "field is num");

	private FieldTrait(int bitPos_, String description_) {
		bitPos = bitPos_;
		description = description_;
	}

	@Override
	public boolean get(Short bits) {
		return ((bits >>> bitPos) & 1) == 1;
	}

	@Override
	public Short mask() {
		return (short)(1 << bitPos);
	}

	@Override
	public int bitPos() {
		return bitPos;
	}

	@Override
	public String description() {
		return description;
	}

	private final int bitPos;
	private final String description;
}
