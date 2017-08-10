/*
 * Copyright (c) 2017 Alex Dubov <oakad@yahoo.com>
 *
 * This file is made available under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package udentric.mysql.classic.message.server;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import java.util.EnumMap;

public abstract class Any {
	protected <F extends Enum<F>> Any(
		FieldAccess<F> instanceAcc_
	) {
		instanceAcc = instanceAcc_;
		values = new Object[instanceAcc.fields.length];
	}

	public boolean decode(ByteBuf in) {
		if (decoded)
			return true;

		return false;
	}

	public interface Field {}

	protected static class FieldAccess<F extends Enum<F>> {
		FieldAccess(Class<F> fieldEnumCls_) {
			fieldEnumCls = fieldEnumCls_;
			fields = fieldEnumCls.getEnumConstants();

			ImmutableMap.Builder<
				String, F
			> builder = ImmutableMap.builder();

			for (F f: fields)
				builder.put(f.name(), f);

			fieldNames = builder.build();
                }

		private final Class<F> fieldEnumCls;
		private final F[] fields;
		private final ImmutableMap<String, F> fieldNames;
	}

	protected final FieldAccess instanceAcc;
	protected final Object[] values;
	private int decoderFieldPos = 0;
	private boolean decoded = false;
	private ByteBuf leftOvers;
}
