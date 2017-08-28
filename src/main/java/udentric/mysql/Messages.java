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

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Messages {
	private Messages() {
	}

	public static String getString(String key) {
		if (RESOURCE_BUNDLE == null) {
			throw new RuntimeException(
				"Localized messages from resource bundle '"
				+ BUNDLE_NAME
				+ "' not loaded during initialization of driver."
			);
		}

		if (key == null) {
			throw new IllegalArgumentException(
				"Message key can not be null"
			);
		}

		try {
			String message = RESOURCE_BUNDLE.getString(key);
			if (message == null) {
				message = "Missing error message for key '"
					  + key + "'";
			}

			return message;
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}

	public static String getString(String key, Object... args) {
		return MessageFormat.format(getString(key), args);
	}

	private static final String BUNDLE_NAME = "udentric.mysql.LocalizedErrorMessages";
	private static final ResourceBundle RESOURCE_BUNDLE;

	static {
		ResourceBundle temp = null;

		try {
			temp = ResourceBundle.getBundle(
				BUNDLE_NAME, Locale.getDefault(),
				Messages.class.getClassLoader()
			);
		} catch (Throwable t) {
			try {
				temp = ResourceBundle.getBundle(BUNDLE_NAME);
			} catch (Throwable t2) {
				RuntimeException rt = new RuntimeException(
					"Can't load resource bundle due to underlying exception",
					t2
				);
				rt.addSuppressed(t);
				throw rt;
			}
		} finally {
			RESOURCE_BUNDLE = temp;
		}
	}        
}
