/*
 * Copyright (c) 2017 - 2018 Alex Dubov <oakad@yahoo.com>
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.CharBuffer;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import udentric.mysql.classic.FieldImpl;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.type.AdapterState;
import udentric.mysql.classic.type.ValueAdapter;
import udentric.mysql.classic.type.TypeId;

public class AnyTemporalString implements ValueAdapter<TemporalAccessor> {
        public AnyTemporalString(TypeId id_) {
                id = id_;
        }
		@Override
	public TypeId typeId() {
		return id;
	}

	@Override
	public void encodeValue(
		ByteBuf dst, TemporalAccessor value, AdapterState state,
		int bufSoftLimit, FieldImpl fld
	) {
		int flag = TemporalQueries.localDate().queryFrom(
			value
		) != null ? 2 : 0;
		flag |= TemporalQueries.localTime().queryFrom(
			value
		) != null ? 1 : 0;

		ByteBuf valBuf = ByteBufUtil.encodeString(
			state.alloc,
			CharBuffer.wrap(FORMATTERS[flag].format(value)),
			fld.encoding.charset
		);
		Packet.writeIntLenenc(dst, valBuf.readableBytes());
		dst.writeBytes(valBuf);
		valBuf.release();
		state.markAsDone();
	}

	@Override
	public TemporalAccessor decodeValue(
		TemporalAccessor dst, ByteBuf src, AdapterState state,
		FieldImpl fld
	) {
		Integer sz = state.get();

		if (sz == null) {
			sz = Packet.readIntLenencSafe(src);
			if (sz < 0)
				return null;
		}

		if (src.readableBytes() >= sz) {
			state.markAsDone();
			return MYSQL_DATE_TIME.parse(src.readCharSequence(
				sz, fld.encoding.charset
			));	
		} else {
			state.set(sz);
			return null;
		}
	}

	public static final DateTimeFormatter MYSQL_ILLEGAL
	= new DateTimeFormatterBuilder().appendLiteral(
		"null"
	).toFormatter();

	public static final DateTimeFormatter MYSQL_DATE
	= DateTimeFormatter.ISO_LOCAL_DATE;

	public static final DateTimeFormatter MYSQL_TIME
	= new DateTimeFormatterBuilder().appendValue(
		ChronoField.HOUR_OF_DAY, 2
	).appendLiteral(':').appendValue(
		ChronoField.MINUTE_OF_HOUR, 2
	).appendLiteral(':').appendValue(
		ChronoField.SECOND_OF_MINUTE, 2
	).optionalStart().appendFraction(
		ChronoField.NANO_OF_SECOND, 0, 6, true
	).toFormatter();

	public static final DateTimeFormatter MYSQL_DATE_TIME
	= new DateTimeFormatterBuilder().append(
		MYSQL_DATE
	).optionalStart().appendLiteral(' ').append(
		MYSQL_TIME
	).toFormatter();

	public static final DateTimeFormatter[] FORMATTERS = {
		MYSQL_ILLEGAL,
		MYSQL_TIME,
		MYSQL_DATE,
		MYSQL_DATE_TIME
	};

	private final TypeId id;
}
