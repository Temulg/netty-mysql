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

package udentric.mysql.classic.type;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BinaryAdapterSelector {
	public abstract <T> BinaryAdapter<T> get(Class<T> cls);

	public abstract <T> BinaryAdapter<T> find(Object obj);

	protected BinaryAdapter<?> findAdapter(
		Object obj,
		ImmutableMap<Class<?>, BinaryAdapter<?>> staticAdapterMap
	) {
		return dynamicAdapterMap.computeIfAbsent(
			obj.getClass(), cls -> {
				Iterator<
					Map.Entry<Class<?>, BinaryAdapter<?>>
				> iter = staticAdapterMap.entrySet().iterator();
				
				while (iter.hasNext()) {
					Map.Entry<
						Class<?>, BinaryAdapter<?>
					> entry = iter.next();

					if (entry.getKey().isInstance(obj)) {
						return entry.getValue();
					}
				}
				return null;
			}
		);
	}

	protected final ConcurrentHashMap<
		Class<?>, BinaryAdapter<?>
	> dynamicAdapterMap = new ConcurrentHashMap<>();
}
