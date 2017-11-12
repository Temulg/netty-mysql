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

import io.netty.buffer.ByteBuf;
import udentric.mysql.classic.FieldImpl;

class LongAdapter implements Adapter {
	public LongAdapter() {
		Adapter nat = new NativeVal();
		adapters = ImmutableMap.<
			Class<?>, Adapter
		>builder().put(
			Long.TYPE, nat
		).put(
			Long.class, nat
		).put(
			String.class, new StringVal()
		).build();
	}

	@Override
	public <T> T textValueDecode(
		ByteBuf src, int offset, int length, Class<T> cls,
		FieldImpl fld
	) {
		Adapter a = findAdapterForClass(cls);
		return a.textValueDecode(src, offset, length, cls, fld);
	}

	@Override
	public <T> Adapter getAdapterForClass(Class<T> cls) {
		return adapters.get(cls);
	}

	private static class NativeVal implements Adapter {
		@SuppressWarnings("unchecked")
		@Override
		public <T> T textValueDecode(
			ByteBuf src, int offset, int length, Class<T> cls,
			FieldImpl fld
		) {
			String s = src.getCharSequence(
				src.readerIndex() + offset, length,
				fld.encoding.charset
			).toString();
	
			return (T)(Long)Long.parseLong(s);
		}
	}

	private static class StringVal implements Adapter {
		@SuppressWarnings("unchecked")
		@Override
		public <T> T textValueDecode(
			ByteBuf src, int offset, int length, Class<T> cls,
			FieldImpl fld
		) {
			String s = src.getCharSequence(
				src.readerIndex() + offset, length,
				fld.encoding.charset
			).toString();
	
			return (T)s;
		}
	}

	final ImmutableMap<Class<?>, Adapter> adapters;
}
