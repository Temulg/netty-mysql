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

public abstract class AdapterSelector {
	public <T> ValueAdapter<T> get() {
		return get(null);
	}

	public abstract <T> ValueAdapter<T> get(Class<T> cls);

	public abstract <T> ValueAdapter<T> find(Class<T> cls);

	protected ValueAdapter<?> findAdapter(
		Class<?> cls,
		ImmutableMap<Class<?>, ValueAdapter<?>> staticAdapterMap
	) {
		return dynamicAdapterMap.computeIfAbsent(
			cls, cls_ -> {
				Iterator<
					Map.Entry<Class<?>, ValueAdapter<?>>
				> iter = staticAdapterMap.entrySet().iterator();
				
				while (iter.hasNext()) {
					Map.Entry<
						Class<?>, ValueAdapter<?>
					> entry = iter.next();

					if (entry.getKey().isAssignableFrom(cls)) {
						return entry.getValue();
					}
				}
				return null;
			}
		);
	}

	protected final ConcurrentHashMap<
		Class<?>, ValueAdapter<?>
	> dynamicAdapterMap = new ConcurrentHashMap<>();
}
