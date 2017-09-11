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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class Config {
	public static class Builder {
		private Builder() {
		}

		public <T> Builder withValue(Key k, T val_) {
			Object val = k.accessor.fromObject(val_);
			if (val != null)
				values.put(k, val);

			return this;
		}

		public Config build() {
			return new Config(values);
		}

		private final EnumMap<
			Key, Object
		> values = new EnumMap<>(Key.class);
	}

	public static Builder empty() {
		return new Builder();
	}

	public static Builder fromEnvironment(boolean useSysEnv) {
		Builder cfg = new Builder();
		Properties props = System.getProperties();
		Map<String, String> env = useSysEnv
			? System.getenv()
			: Collections.emptyMap();

		for (Key key: Key.values()) {
			key.sysProp.ifPresent(prop -> {
				Object val = key.accessor.fromString(
					props.getProperty(prop)
				);
				if (val != null)
					cfg.values.put(key, val);
			});

			if (!useSysEnv)
				continue;

			key.envVar.ifPresent(var -> {
				Object val = key.accessor.fromString(
					env.get(var)
				);
				if (val != null)
					cfg.values.put(key, val);
			});
		}

		return cfg;
	}

	
	private Config(EnumMap<Key, Object> values_) {
		values = values_;
	}

	@SuppressWarnings("unchecked")
	public <T> T getOrDefault(Key key, T defVal) {
		return (T)key.accessor.getOrDefault(values, key, defVal);
	}

	public boolean containsKey(Key key) {
		return values.containsKey(key);
	}

	private static abstract class Value {
		Object getOrDefault(
			EnumMap<Key, Object> values,
			Key key, Object defValue
		) {
			return values.getOrDefault(key, defValue);
		}

		abstract Object fromString(String val);

		abstract Object fromObject(Object val);
	}

	private static final Value BOOLEAN_VALUE = new Value() {
		@Override
		Object fromString(String val) {
			return Boolean.parseBoolean(val);
		}

		@Override
		Object fromObject(Object val) {
			if (val instanceof Boolean)
				return val;
			else if (val instanceof Number)
				return ((Number)val).intValue() > 0;
			else if (val instanceof CharSequence)
				return Boolean.parseBoolean(val.toString());
			else
				return null;
		}
	};

	private static final Value INTEGER_VALUE = new Value() {
		@Override
		Object fromString(String val) {
			try {
				return Integer.parseInt(val);
			} catch (Exception e) {
				return null;
			}
		}

		@Override
		Object fromObject(Object val) {
			if (val instanceof Number)
				return ((Number)val).intValue();
			else
				return null;
		}
	};

	private static final Value STRING_VALUE = new Value() {
		@Override
		Object fromString(String val) {
			return val;
		}

		@Override
		Object fromObject(Object val) {
			return val.toString();
		}
	};

	public enum Key {
		PROTOCOL(STRING_VALUE),
		PATH(STRING_VALUE),
		TYPE(STRING_VALUE),
		HOST(STRING_VALUE, "udentric.mysql.host", "MYSQL_HOST"),
		TCP_PORT(
			INTEGER_VALUE, "udentric.mysql.tcp_port",
			"MYSQL_TCP_PORT"
		),
		UNIX_PORT(
			STRING_VALUE, "udentric.mysql.unix_port",
			"MYSQL_UNIX_PORT"
		),
		DBNAME(STRING_VALUE),
		ADDRESS(STRING_VALUE),
		PRIORITY(STRING_VALUE),
		user(STRING_VALUE, "udentric.mysql.user", "USER"),
		password(STRING_VALUE, "udentric.mysql.password", "MYSQL_PWD"),
		interactiveClient(BOOLEAN_VALUE),
		maintainTimeStats(BOOLEAN_VALUE),
		paranoid(BOOLEAN_VALUE),
		localSocketAddress(STRING_VALUE),
		maxPacketSize(INTEGER_VALUE);

		private Key(Value accessor_) {
			accessor = accessor_;
			envVar = Optional.empty();
			sysProp = Optional.empty();
		}

		private Key(Value accessor_, String sysProp_, String envVar_) {
			accessor = accessor_;
			sysProp = Optional.ofNullable(sysProp_);
			envVar = Optional.ofNullable(envVar_);
		}

		private final Value accessor;
		private final Optional<String> sysProp;
		private final Optional<String> envVar;
	}

	private final EnumMap<Key, Object> values;
}
