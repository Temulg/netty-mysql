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

package udentric.mysql.classic.type.binary;

import com.google.common.collect.ImmutableMap;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import udentric.mysql.classic.type.BinaryAdapter;
import udentric.mysql.classic.type.BinaryAdapterSelector;
import udentric.mysql.classic.type.TypeId;

public class T0253Selector extends BinaryAdapterSelector {
	@Override
	@SuppressWarnings("unchecked")
	public <T> BinaryAdapter<T> get(Class<T> cls) {
		return (BinaryAdapter<T>)(
			cls != null ? ADAPTERS.get(cls) : defaultAdapter
		);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> BinaryAdapter<T> find(Object obj) {
		return (BinaryAdapter<T>)findAdapter(obj, ADAPTERS);
	}

	private final BinaryAdapter<?> defaultAdapter = new AnyByteArray(
		TypeId.VAR_STRING
	);

	private final ImmutableMap<
		Class<?>, BinaryAdapter<?>
	> ADAPTERS = ImmutableMap.<
		Class<?>, BinaryAdapter<?>
	>builder().put(
		byte[].class, defaultAdapter
	).put(
		ScatteringByteChannel.class,
		new AnyNioReadChannel(TypeId.VAR_STRING)
	).put(
		GatheringByteChannel.class,
		new AnyNioWriteChannel(TypeId.VAR_STRING)
	).build();
}
