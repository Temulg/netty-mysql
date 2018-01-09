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

package udentric.mysql;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.SucceededFuture;
import java.util.EnumMap;

public class MetadataQueries {
	private MetadataQueries() {
	}

	public static Future<PreparedStatement> importedKeys(Channel ch) {
		return getMap(ch).get(QueryType.IMPORTED_KEYS, ch);
	}

	private static QueryMap getMap(Channel ch) {
		Attribute<QueryMap> attr = ch.attr(METADATA_QUERY_MAP);
		QueryMap qm = attr.get();
		if (qm == null)
			return attr.setIfAbsent(new QueryMap(ch));

		return qm;
	}

	private static enum QueryType {
		IMPORTED_KEYS;
	}

	private static class QueryMap {
		QueryMap(Channel ch_) {
			ch = ch_;
		}

		synchronized Future<PreparedStatement> get(
			QueryType type, Channel ch
		) {
			Future<PreparedStatement> p = stmtMap.get(type);
			if (p != null)
				return p;

			Promise<PreparedStatement> pp = ch.eventLoop().<
				PreparedStatement
			>newPromise();
			pp.addListener(fp -> {
				stmtPrepared(type, fp);
			});
			stmtMap.put(type, pp);

			
			return pp;
		}

		synchronized void stmtPrepared(
			QueryType type, Future<?> fp
		) {
			Future<PreparedStatement> p = stmtMap.get(type);
			if (p != null) {
				if (p instanceof SucceededFuture)
					return;
			}

			if (!fp.isSuccess()) {
				stmtMap.remove(type);
				return;
			}

			PreparedStatement pp = (PreparedStatement)fp.getNow();
			stmtMap.put(
				type, ch.eventLoop().newSucceededFuture(pp)
			);
		}

		private final Channel ch;
		private final EnumMap<
			QueryType, Future<PreparedStatement>
		> stmtMap = new EnumMap<>(QueryType.class);
	}

	private static final AttributeKey<
		QueryMap
	> METADATA_QUERY_MAP = AttributeKey.valueOf(
		"udentric.mysql.MetadataQueries.QueryMap"
	);
}
