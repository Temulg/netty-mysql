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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.EnumMap;
import java.util.Properties;
import java.util.Collections;

import com.google.common.base.MoreObjects;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import udentric.mysql.util.Throwables;

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
			String name = key.property();
			if (!name.isEmpty()) {
				Object val = key.accessor.fromString(
					props.getProperty(name)
				);
				if (val != null)
					cfg.values.put(key, val);
			}

			if (!useSysEnv)
				continue;

			name = key.envVar();
			if (!name.isEmpty()) {
				Object val = key.accessor.fromString(
					env.get(name)
				);
				if (val != null)
					cfg.values.put(key, val);
			}
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
		if (values.containsKey(key))
			return true;

		key.update(values, null);
		return values.containsKey(key);
	}

	@Override
	public String toString() {
		MoreObjects.ToStringHelper h = MoreObjects.toStringHelper(this);
		values.forEach((k, v) -> {
			h.add(k.name(), v.toString());
		});
		return h.toString();
	}

	private static abstract class Value {
		Object getOrDefault(
			EnumMap<Key, Object> values,
			Key key, Object defValue
		) {
			if (values.containsKey(key))
				return values.get(key);
			else
				return key.update(values, defValue);
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

	private static final Value BYTE_ARRAY_VALUE = new Value() {
		@Override
		Object fromString(String val) {
			return BaseEncoding.base16().decode(val);
		}

		@Override
		Object fromObject(Object val) {
			if (val instanceof byte[])
				return val;
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

	private static final Value URL_VALUE = new Value() {
		@Override
		Object fromString(String val) {
			try {
				return new URL(val);
			} catch (MalformedURLException e) {}

			Path p = Paths.get(val).toAbsolutePath();
			try {
				return new URL(
					"file", null, p.toString()
				);
			} catch (MalformedURLException e) {
				throw Throwables.propagate(e);
			}
		}

		@Override
		Object fromObject(Object val) {
			if (val instanceof URL)
				return val;
			else
				return null;
		}
	};

	public enum Key {
		HOST(STRING_VALUE) {
			@Override
			String property() {
				return "udentric.mysql.host";
			}

			@Override
			String envVar() {
				return "MYSQL_HOST";
			}
		},
		TCP_PORT(INTEGER_VALUE) {
			@Override
			String property() {
				return "udentric.mysql.tcp_port";
			}

			@Override
			String envVar() {
				return "MYSQL_TCP_PORT";
			}
		},
		UNIX_PORT(STRING_VALUE) {
			@Override
			String property() {
				return "udentric.mysql.unix_port";
			}

			@Override
			String envVar() {
				return "MYSQL_UNIX_PORT";
			}
		},
		DBNAME(STRING_VALUE),
		USER(STRING_VALUE) {
			@Override
			String property() {
				return "udentric.mysql.user";
			}

			@Override
			String envVar() {
				return "USER";
			}
		},
		PASSWORD(STRING_VALUE) {
			@Override
			String property() {
				return "udentric.mysql.password";
			}

			@Override
			String envVar() {
				return "MYSQL_PWD";
			}
		},
		ENABLE_SSL(BOOLEAN_VALUE),
		CHARACTER_ENCODING(STRING_VALUE),
		MAX_PACKET_SIZE(INTEGER_VALUE),
		SERVER_PUBLIC_KEY_URL(URL_VALUE),
		SERVER_PUBLIC_KEY(BYTE_ARRAY_VALUE) {
			@Override
			Object update(
				EnumMap<Key, Object> values, Object defValue
			) {
				URL u = (URL)values.get(SERVER_PUBLIC_KEY_URL);
				if (u == null)
					return defValue;

				try (InputStream s = u.openStream()) {
					byte[] b = ByteStreams.toByteArray(s);
					values.put(SERVER_PUBLIC_KEY, b);
					return b;
				} catch (IOException e) {
					return defValue;
				}
			}
		};

		Key(Value accessor_) {
			accessor = accessor_;
		}

		String property() {
			return "";
		}

		String envVar() {
			return "";
		}

		Object update(EnumMap<Key, Object> values, Object defValue) {
			return defValue;
		}

		private final Value accessor;
	}

	private final EnumMap<Key, Object> values;
}
