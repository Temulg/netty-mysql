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

package udentric.mysql.classic.type;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeUtils {
	private TimeUtils() {
	}

	public static byte[] encodeBinary(LocalDateTime value) {
		long micros = TimeUnit.MICROSECONDS.convert(
			value.getNano(), TimeUnit.NANOSECONDS
		);
		byte[] valBuf = micros > 0 ? new byte[11] : new byte[7];

		valBuf[0] = (byte)value.getYear();
		valBuf[1] = (byte)(value.getYear() >>> 8);
		valBuf[2] = (byte)value.getMonth().getValue();
		valBuf[3] = (byte)value.getDayOfMonth();
		valBuf[4] = (byte)value.getHour();
		valBuf[5] = (byte)value.getMinute();
		valBuf[6] = (byte)value.getSecond();

		if (micros > 0)
			longToBytes4LE(valBuf, micros, 7);

		return valBuf;
	}

	public static LocalDateTime decodeDateTime(byte[] valBuf) {
		if (valBuf.length < 7)
			throw new IllegalArgumentException(
				"not enough bytes for value representation"
			);

		int year = ((int)valBuf[1] << 8) | (int)valBuf[0];

		int micros = 0;

		if (valBuf.length >= 11)
			micros = bytesToIntLE(valBuf, 7);

		return LocalDateTime.of(
			year, valBuf[2], valBuf[3], valBuf[4], valBuf[5],
			valBuf[6], (int)TimeUnit.MICROSECONDS.toNanos(micros)
		);
	}

	public static byte[] encodeBinary(LocalDate value) {
		byte[] valBuf = new byte[4];

		valBuf[0] = (byte)value.getYear();
		valBuf[1] = (byte)(value.getYear() >>> 8);
		valBuf[2] = (byte)value.getMonth().getValue();
		valBuf[3] = (byte)value.getDayOfMonth();
		return valBuf;
	}

	public static LocalDate decodeDate(byte[] valBuf) {
		if (valBuf.length < 4)
			throw new IllegalStateException(
				"not enough bytes for value representation"
			);

		int year = ((int)valBuf[1] << 8) | (int)valBuf[0];

		return LocalDate.of(year, valBuf[2], valBuf[3]);
	}

	public static byte[] encodeBinary(LocalTime value) {
		long micros = TimeUnit.MICROSECONDS.convert(
			value.getNano(), TimeUnit.NANOSECONDS
		);
		byte[] valBuf = micros > 0 ? new byte[12] : new byte[8];

		valBuf[5] = (byte)value.getHour();
		valBuf[6] = (byte)value.getMinute();
		valBuf[7] = (byte)value.getSecond();

		if (micros > 0)
			longToBytes4LE(valBuf, micros, 8);

		return valBuf;
	}

	public static LocalTime decodeTime(byte[] valBuf) {
		if (valBuf.length < 8)
			throw new IllegalArgumentException(
				"not enough bytes for value representation"
			);

		if (valBuf[0] != 0)
			throw new IllegalArgumentException(
				"negative time value"
			);

		if (bytesToIntLE(valBuf, 1) != 0)
			throw new IllegalArgumentException(
				"time value not representable"
			);

		long nanos = 0;
		if (valBuf.length >= 12)
			nanos = TimeUnit.NANOSECONDS.convert(
				bytesToIntLE(valBuf, 8), TimeUnit.MICROSECONDS
			);

		return LocalTime.of(
			valBuf[5], valBuf[6], valBuf[7], (int)nanos
		);
	}

	public static byte[] encodeBinary(Duration value) {
		long seconds = Math.abs(value.getSeconds());
		long micros = TimeUnit.MICROSECONDS.convert(
			value.getNano(), TimeUnit.NANOSECONDS
		);
		byte[] valBuf = micros > 0 ? new byte[12] : new byte[8];

		valBuf[0] = value.isNegative() ? (byte)1 : 0;

		long t = TimeUnit.DAYS.convert(seconds, TimeUnit.SECONDS);

		if (t > 0) {
			longToBytes4LE(valBuf, t, 1);
			seconds -= TimeUnit.DAYS.toSeconds(t);
		}

		t = TimeUnit.HOURS.convert(seconds, TimeUnit.SECONDS);
		valBuf[5] = (byte)t;
		seconds -= TimeUnit.HOURS.toSeconds(t);

		t = TimeUnit.MINUTES.convert(seconds, TimeUnit.SECONDS);
		valBuf[6] = (byte)t;
		seconds -= TimeUnit.MINUTES.toSeconds(t);

		valBuf[7] = (byte)seconds;

		if (micros > 0)
			longToBytes4LE(valBuf, micros, 8);

		return valBuf;
	}

	public static String formatString(Duration value) {
		if (value.isZero())
			return "00:00:00";

		long seconds = Math.abs(value.getSeconds());
		int nanos = value.getNano();

		long hours = TimeUnit.HOURS.convert(seconds, TimeUnit.SECONDS);
		seconds -= TimeUnit.HOURS.toSeconds(hours);

		long minutes = TimeUnit.MINUTES.convert(
			seconds, TimeUnit.SECONDS
		);
		seconds -= TimeUnit.MINUTES.toSeconds(minutes);
		
		StringBuilder buf = new StringBuilder(24);

		if (value.isNegative())
			buf.append('-');

		buf.append(
			hours
		).append(':').append(
			minutes
		).append(':').append(
			seconds
		);

		if (nanos > 0) {
			buf.append('.');
			int scale = NANOS_FIELD_SCALE;

			for (int pos = 0; pos < 6; pos++) {
				int dg = nanos / scale;
				buf.append(Character.forDigit(dg, 10));

				nanos -= dg * scale;
				if (nanos == 0)
					break;
				scale /= 10;
			}
		}
		return buf.toString();
	}

	public static Duration decodeDuration(byte[] valBuf) {
		if (valBuf.length < 8)
			throw new IllegalArgumentException(
				"not enough bytes for value representation"
			);

		long seconds = TimeUnit.SECONDS.convert(
			bytesToIntLE(valBuf, 1),
			TimeUnit.DAYS
		);
		seconds += TimeUnit.SECONDS.convert(
			valBuf[5], TimeUnit.HOURS
		);
		seconds += TimeUnit.SECONDS.convert(
			valBuf[6], TimeUnit.MINUTES
		);
		seconds += valBuf[7];

		long nanos = 0;
		if (valBuf.length >= 12)
			nanos = TimeUnit.NANOSECONDS.convert(
				bytesToIntLE(valBuf, 8), TimeUnit.MICROSECONDS
			);

		if (valBuf[0] != 0) {
			seconds = -seconds;
			nanos = -nanos;
		}

		return Duration.ofSeconds(seconds, nanos);
	}

	public static Duration parseDuration(CharSequence value) {
		Matcher m = MYSQL_DURATION.matcher(value);
		if (!m.matches())
			throw new IllegalArgumentException(String.format(
				"value %s can not be parsed as duration",
				value
			));

		long seconds = TimeUnit.HOURS.toSeconds(
			Long.parseUnsignedLong(m.group(2))
		);
		seconds += TimeUnit.MINUTES.toSeconds(
			Long.parseUnsignedLong(m.group(3))
		);

		String gSec = m.group(4);
		int nanos = 0;

		if (gSec != null) {
			seconds += Long.parseUnsignedLong(gSec);
			nanos = fracSecToNanos(m.group(5));
		}

		if ("-".equals(m.group(1))) {
			seconds = -seconds;
			nanos = -nanos;
		}

		return Duration.ofSeconds(seconds, nanos);
	}

	public static void longToBytes4LE(byte[] dst, long src, int offset) {
		dst[offset] = (byte)src;
		dst[offset + 1] = (byte)(src >>> 8);
		dst[offset + 2] = (byte)(src >>> 16);
		dst[offset + 3] = (byte)(src >>> 24);
	}

	public static int bytesToIntLE(byte[] src, int offset) {
		int v = (int)src[offset] & 0xff;
		v |= ((int)src[offset + 1] & 0xff) << 8;
		v |= ((int)src[offset + 2] & 0xff) << 8;
		v |= ((int)src[offset + 3] & 0xff) << 8;
		return v;
	}

	private static int fracSecToNanos(String sFrac) {
		if (sFrac == null)
			return 0;

		int v = 0;
		int scale = NANOS_FIELD_SCALE;

		for (int pos = 0; pos < sFrac.length(); pos++) {
			v += scale * Character.digit(sFrac.charAt(pos), 10);
			scale = scale / 10;
		}

		return v;
	}

	private static final Pattern MYSQL_DURATION = Pattern.compile(
		"(-?)(\\d+):(\\d+)(?::(\\d+)(?:\\.(\\d+))?)?"
	);
	private static final int NANOS_FIELD_SCALE
	= (int)TimeUnit.MILLISECONDS.toNanos(100);
}
