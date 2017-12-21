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

import com.google.common.base.MoreObjects;
import java.nio.charset.Charset;

public class Encoding {
	private Encoding(
		int mysqlId_, String mysqlCharset_, String mysqlCollation_,
		String javaCharsetName
	) {
		mysqlId = mysqlId_;
		mysqlCharset = mysqlCharset_;
		mysqlCollation = mysqlCollation_;
		charset = Charset.forName(javaCharsetName);
	}

	public boolean compatible(Encoding other) {
		return charset.equals(other.charset);
	}

	@Override
	public boolean equals(Object other_) {
		if (!(other_ instanceof Encoding))
			return false;

		Encoding other = (Encoding)other_;

		return mysqlId == other.mysqlId;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add(
			"mysqlId", mysqlId
		).add(
			"mysqlCharset", mysqlCharset
		).add(
			"mysqlCollation", mysqlCollation
		).add(
			"charset", charset
		).toString();
	}

	public final int mysqlId;
	public final String mysqlCharset;
	public final String mysqlCollation;
	public final Charset charset;

	public static Encoding forId(int id) {
		return ID_TO_ENCODING[id];
	}

	private static final Encoding[] ID_TO_ENCODING = new Encoding[]{
		null,
		new Encoding(1, "big5", "big5_chinese_ci", "Big5"),
		new Encoding(2, "latin2", "latin2_czech_cs", "ISO-8859-2"),
		new Encoding(3, "dec8", "dec8_swedish_ci", "US-ASCII"),
		new Encoding(4, "cp850", "cp850_general_ci", "IBM850"),
		new Encoding(5, "latin1", "latin1_german1_ci", "ISO-8859-1"),
		new Encoding(6, "hp8", "hp8_english_ci", "US-ASCII"),
		new Encoding(7, "koi8r", "koi8r_general_ci", "KOI8-R"),
		new Encoding(8, "latin1", "latin1_swedish_ci", "ISO-8859-1"),
		new Encoding(9, "latin2", "latin2_general_ci", "ISO-8859-2"),
		new Encoding(10, "swe7", "swe7_swedish_ci", "US-ASCII"),
		new Encoding(11, "ascii", "ascii_general_ci", "US-ASCII"),
		new Encoding(12, "ujis", "ujis_japanese_ci", "EUC-JP"),
		new Encoding(13, "sjis", "sjis_japanese_ci", "Shift_JIS"),
		new Encoding(14, "cp1251", "cp1251_bulgarian_ci", "windows-1251"),
		new Encoding(15, "latin1", "latin1_danish_ci", "ISO-8859-1"),
		new Encoding(16, "hebrew", "hebrew_general_ci", "windows-1255"),
		null,
		new Encoding(18, "tis620", "tis620_thai_ci", "TIS-620"),
		new Encoding(19, "euckr", "euckr_korean_ci", "EUC-KR"),
		new Encoding(20, "latin7", "latin7_estonian_cs", "ISO-8859-13"),
		new Encoding(21, "latin2", "latin2_hungarian_ci", "ISO-8859-2"),
		new Encoding(22, "koi8u", "koi8u_general_ci", "KOI8-U"),
		new Encoding(23, "cp1251", "cp1251_ukrainian_ci", "windows-1251"),
		new Encoding(24, "gb2312", "gb2312_chinese_ci", "GB2312"),
		new Encoding(25, "greek", "greek_general_ci", "windows-1253"),
		new Encoding(26, "cp1250", "cp1250_general_ci", "windows-1250"),
		new Encoding(27, "latin2", "latin2_croatian_ci", "ISO-8859-2"),
		new Encoding(28, "gbk", "gbk_chinese_ci", "GBK"),
		new Encoding(29, "cp1257", "cp1257_lithuanian_ci", "windows-1257"),
		new Encoding(30, "latin5", "latin5_turkish_ci", "ISO-8859-9"),
		new Encoding(31, "latin1", "latin1_german2_ci", "ISO-8859-1"),
		new Encoding(32, "armscii8", "armscii8_general_ci", "US-ASCII"),
		new Encoding(33, "utf8", "utf8_general_ci", "UTF-8"),
		new Encoding(34, "cp1250", "cp1250_czech_cs", "windows-1250"),
		new Encoding(35, "ucs2", "ucs2_general_ci", "UTF-16"),
		new Encoding(36, "cp866", "cp866_general_ci", "IBM866"),
		new Encoding(37, "keybcs2", "keybcs2_general_ci", "UTF-16"),
		new Encoding(38, "macce", "macce_general_ci", "x-MacCentralEurope"),
		new Encoding(39, "macroman", "macroman_general_ci", "x-MacRoman"),
		new Encoding(40, "cp852", "cp852_general_ci", "IBM852"),
		new Encoding(41, "latin7", "latin7_general_ci", "ISO-8859-13"),
		new Encoding(42, "latin7", "latin7_general_cs", "ISO-8859-13"),
		new Encoding(43, "macce", "macce_bin", "x-MacCentralEurope"),
		new Encoding(44, "cp1250", "cp1250_croatian_ci", "windows-1250"),
		new Encoding(45, "utf8mb4", "utf8mb4_general_ci", "UTF-8"),
		new Encoding(46, "utf8mb4", "utf8mb4_bin", "UTF-8"),
		new Encoding(47, "latin1", "latin1_bin", "ISO-8859-1"),
		new Encoding(48, "latin1", "latin1_general_ci", "ISO-8859-1"),
		new Encoding(49, "latin1", "latin1_general_cs", "ISO-8859-1"),
		new Encoding(50, "cp1251", "cp1251_bin", "windows-1251"),
		new Encoding(51, "cp1251", "cp1251_general_ci", "windows-1251"),
		new Encoding(52, "cp1251", "cp1251_general_cs", "windows-1251"),
		new Encoding(53, "macroman", "macroman_bin", "x-MacRoman"),
		new Encoding(54, "utf16", "utf16_general_ci", "UTF-16"),
		new Encoding(55, "utf16", "utf16_bin", "UTF-16"),
		new Encoding(56, "utf16le", "utf16le_general_ci", "UTF-16LE"),
		new Encoding(57, "cp1256", "cp1256_general_ci", "windows-1256"),
		new Encoding(58, "cp1257", "cp1257_bin", "windows-1257"),
		new Encoding(59, "cp1257", "cp1257_general_ci", "windows-1257"),
		new Encoding(60, "utf32", "utf32_general_ci", "UTF-32"),
		new Encoding(61, "utf32", "utf32_bin", "UTF-32"),
		new Encoding(62, "utf16le", "utf16le_bin", "UTF-16LE"),
		new Encoding(63, "binary", "binary", "UTF-8"),
		new Encoding(64, "armscii8", "armscii8_bin", "US-ASCII"),
		new Encoding(65, "ascii", "ascii_bin", "US-ASCII"),
		new Encoding(66, "cp1250", "cp1250_bin", "windows-1250"),
		new Encoding(67, "cp1256", "cp1256_bin", "windows-1256"),
		new Encoding(68, "cp866", "cp866_bin", "IBM866"),
		new Encoding(69, "dec8", "dec8_bin", "US-ASCII"),
		new Encoding(70, "greek", "greek_bin", "windows-1253"),
		new Encoding(71, "hebrew", "hebrew_bin", "windows-1255"),
		new Encoding(72, "hp8", "hp8_bin", "US-ASCII"),
		new Encoding(73, "keybcs2", "keybcs2_bin", "UTF-16"),
		new Encoding(74, "koi8r", "koi8r_bin", "KOI8-R"),
		new Encoding(75, "koi8u", "koi8u_bin", "KOI8-U"),
		null,
		new Encoding(77, "latin2", "latin2_bin", "ISO-8859-2"),
		new Encoding(78, "latin5", "latin5_bin", "ISO-8859-9"),
		new Encoding(79, "latin7", "latin7_bin", "ISO-8859-13"),
		new Encoding(80, "cp850", "cp850_bin", "IBM850"),
		new Encoding(81, "cp852", "cp852_bin", "IBM852"),
		new Encoding(82, "swe7", "swe7_bin", "US-ASCII"),
		new Encoding(83, "utf8", "utf8_bin", "UTF-8"),
		new Encoding(84, "big5", "big5_bin", "Big5"),
		new Encoding(85, "euckr", "euckr_bin", "EUC-KR"),
		new Encoding(86, "gb2312", "gb2312_bin", "GB2312"),
		new Encoding(87, "gbk", "gbk_bin", "GBK"),
		new Encoding(88, "sjis", "sjis_bin", "Shift_JIS"),
		new Encoding(89, "tis620", "tis620_bin", "TIS-620"),
		new Encoding(90, "ucs2", "ucs2_bin", "UTF-16"),
		new Encoding(91, "ujis", "ujis_bin", "EUC-JP"),
		new Encoding(92, "geostd8", "geostd8_general_ci", "US-ASCII"),
		new Encoding(93, "geostd8", "geostd8_bin", "US-ASCII"),
		new Encoding(94, "latin1", "latin1_spanish_ci", "IBM850"),
		new Encoding(95, "cp932", "cp932_japanese_ci", "windows-31j"),
		new Encoding(96, "cp932", "cp932_bin", "windows-31j"),
		new Encoding(97, "eucjpms", "eucjpms_japanese_ci", "EUC-JP"),
		new Encoding(98, "eucjpms", "eucjpms_bin", "EUC-JP"),
		new Encoding(99, "cp1250", "cp1250_polish_ci", "windows-1250"),
		null,
		new Encoding(101, "utf16", "utf16_unicode_ci", "UTF-16"),
		new Encoding(102, "utf16", "utf16_icelandic_ci", "UTF-16"),
		new Encoding(103, "utf16", "utf16_latvian_ci", "UTF-16"),
		new Encoding(104, "utf16", "utf16_romanian_ci", "UTF-16"),
		new Encoding(105, "utf16", "utf16_slovenian_ci", "UTF-16"),
		new Encoding(106, "utf16", "utf16_polish_ci", "UTF-16"),
		new Encoding(107, "utf16", "utf16_estonian_ci", "UTF-16"),
		new Encoding(108, "utf16", "utf16_spanish_ci", "UTF-16"),
		new Encoding(109, "utf16", "utf16_swedish_ci", "UTF-16"),
		new Encoding(110, "utf16", "utf16_turkish_ci", "UTF-16"),
		new Encoding(111, "utf16", "utf16_czech_ci", "UTF-16"),
		new Encoding(112, "utf16", "utf16_danish_ci", "UTF-16"),
		new Encoding(113, "utf16", "utf16_lithuanian_ci", "UTF-16"),
		new Encoding(114, "utf16", "utf16_slovak_ci", "UTF-16"),
		new Encoding(115, "utf16", "utf16_spanish2_ci", "UTF-16"),
		new Encoding(116, "utf16", "utf16_roman_ci", "UTF-16"),
		new Encoding(117, "utf16", "utf16_persian_ci", "UTF-16"),
		new Encoding(118, "utf16", "utf16_esperanto_ci", "UTF-16"),
		new Encoding(119, "utf16", "utf16_hungarian_ci", "UTF-16"),
		new Encoding(120, "utf16", "utf16_sinhala_ci", "UTF-16"),
		new Encoding(121, "utf16", "utf16_german2_ci", "UTF-16"),
		new Encoding(122, "utf16", "utf16_croatian_ci", "UTF-16"),
		new Encoding(123, "utf16", "utf16_unicode_520_ci", "UTF-16"),
		new Encoding(124, "utf16", "utf16_vietnamese_ci", "UTF-16"),
		null,
		null,
		null,
		new Encoding(128, "ucs2", "ucs2_unicode_ci", "UTF-16"),
		new Encoding(129, "ucs2", "ucs2_icelandic_ci", "UTF-16"),
		new Encoding(130, "ucs2", "ucs2_latvian_ci", "UTF-16"),
		new Encoding(131, "ucs2", "ucs2_romanian_ci", "UTF-16"),
		new Encoding(132, "ucs2", "ucs2_slovenian_ci", "UTF-16"),
		new Encoding(133, "ucs2", "ucs2_polish_ci", "UTF-16"),
		new Encoding(134, "ucs2", "ucs2_estonian_ci", "UTF-16"),
		new Encoding(135, "ucs2", "ucs2_spanish_ci", "UTF-16"),
		new Encoding(136, "ucs2", "ucs2_swedish_ci", "UTF-16"),
		new Encoding(137, "ucs2", "ucs2_turkish_ci", "UTF-16"),
		new Encoding(138, "ucs2", "ucs2_czech_ci", "UTF-16"),
		new Encoding(139, "ucs2", "ucs2_danish_ci", "UTF-16"),
		new Encoding(140, "ucs2", "ucs2_lithuanian_ci", "UTF-16"),
		new Encoding(141, "ucs2", "ucs2_slovak_ci", "UTF-16"),
		new Encoding(142, "ucs2", "ucs2_spanish2_ci", "UTF-16"),
		new Encoding(143, "ucs2", "ucs2_roman_ci", "UTF-16"),
		new Encoding(144, "ucs2", "ucs2_persian_ci", "UTF-16"),
		new Encoding(145, "ucs2", "ucs2_esperanto_ci", "UTF-16"),
		new Encoding(146, "ucs2", "ucs2_hungarian_ci", "UTF-16"),
		new Encoding(147, "ucs2", "ucs2_sinhala_ci", "UTF-16"),
		new Encoding(148, "ucs2", "ucs2_german2_ci", "UTF-16"),
		new Encoding(149, "ucs2", "ucs2_croatian_ci", "UTF-16"),
		new Encoding(150, "ucs2", "ucs2_unicode_520_ci", "UTF-16"),
		new Encoding(151, "ucs2", "ucs2_vietnamese_ci", "UTF-16"),
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		new Encoding(159, "ucs2", "ucs2_general_mysql500_ci", "UTF-16"),
		new Encoding(160, "utf32", "utf32_unicode_ci", "UTF-32"),
		new Encoding(161, "utf32", "utf32_icelandic_ci", "UTF-32"),
		new Encoding(162, "utf32", "utf32_latvian_ci", "UTF-32"),
		new Encoding(163, "utf32", "utf32_romanian_ci", "UTF-32"),
		new Encoding(164, "utf32", "utf32_slovenian_ci", "UTF-32"),
		new Encoding(165, "utf32", "utf32_polish_ci", "UTF-32"),
		new Encoding(166, "utf32", "utf32_estonian_ci", "UTF-32"),
		new Encoding(167, "utf32", "utf32_spanish_ci", "UTF-32"),
		new Encoding(168, "utf32", "utf32_swedish_ci", "UTF-32"),
		new Encoding(169, "utf32", "utf32_turkish_ci", "UTF-32"),
		new Encoding(170, "utf32", "utf32_czech_ci", "UTF-32"),
		new Encoding(171, "utf32", "utf32_danish_ci", "UTF-32"),
		new Encoding(172, "utf32", "utf32_lithuanian_ci", "UTF-32"),
		new Encoding(173, "utf32", "utf32_slovak_ci", "UTF-32"),
		new Encoding(174, "utf32", "utf32_spanish2_ci", "UTF-32"),
		new Encoding(175, "utf32", "utf32_roman_ci", "UTF-32"),
		new Encoding(176, "utf32", "utf32_persian_ci", "UTF-32"),
		new Encoding(177, "utf32", "utf32_esperanto_ci", "UTF-32"),
		new Encoding(178, "utf32", "utf32_hungarian_ci", "UTF-32"),
		new Encoding(179, "utf32", "utf32_sinhala_ci", "UTF-32"),
		new Encoding(180, "utf32", "utf32_german2_ci", "UTF-32"),
		new Encoding(181, "utf32", "utf32_croatian_ci", "UTF-32"),
		new Encoding(182, "utf32", "utf32_unicode_520_ci", "UTF-32"),
		new Encoding(183, "utf32", "utf32_vietnamese_ci", "UTF-32"),
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		new Encoding(192, "utf8", "utf8_unicode_ci", "UTF-8"),
		new Encoding(193, "utf8", "utf8_icelandic_ci", "UTF-8"),
		new Encoding(194, "utf8", "utf8_latvian_ci", "UTF-8"),
		new Encoding(195, "utf8", "utf8_romanian_ci", "UTF-8"),
		new Encoding(196, "utf8", "utf8_slovenian_ci", "UTF-8"),
		new Encoding(197, "utf8", "utf8_polish_ci", "UTF-8"),
		new Encoding(198, "utf8", "utf8_estonian_ci", "UTF-8"),
		new Encoding(199, "utf8", "utf8_spanish_ci", "UTF-8"),
		new Encoding(200, "utf8", "utf8_swedish_ci", "UTF-8"),
		new Encoding(201, "utf8", "utf8_turkish_ci", "UTF-8"),
		new Encoding(202, "utf8", "utf8_czech_ci", "UTF-8"),
		new Encoding(203, "utf8", "utf8_danish_ci", "UTF-8"),
		new Encoding(204, "utf8", "utf8_lithuanian_ci", "UTF-8"),
		new Encoding(205, "utf8", "utf8_slovak_ci", "UTF-8"),
		new Encoding(206, "utf8", "utf8_spanish2_ci", "UTF-8"),
		new Encoding(207, "utf8", "utf8_roman_ci", "UTF-8"),
		new Encoding(208, "utf8", "utf8_persian_ci", "UTF-8"),
		new Encoding(209, "utf8", "utf8_esperanto_ci", "UTF-8"),
		new Encoding(210, "utf8", "utf8_hungarian_ci", "UTF-8"),
		new Encoding(211, "utf8", "utf8_sinhala_ci", "UTF-8"),
		new Encoding(212, "utf8", "utf8_german2_ci", "UTF-8"),
		new Encoding(213, "utf8", "utf8_croatian_ci", "UTF-8"),
		new Encoding(214, "utf8", "utf8_unicode_520_ci", "UTF-8"),
		new Encoding(215, "utf8", "utf8_vietnamese_ci", "UTF-8"),
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		new Encoding(223, "utf8", "utf8_general_mysql500_ci", "UTF-8"),
		new Encoding(224, "utf8mb4", "utf8mb4_unicode_ci", "UTF-8"),
		new Encoding(225, "utf8mb4", "utf8mb4_icelandic_ci", "UTF-8"),
		new Encoding(226, "utf8mb4", "utf8mb4_latvian_ci", "UTF-8"),
		new Encoding(227, "utf8mb4", "utf8mb4_romanian_ci", "UTF-8"),
		new Encoding(228, "utf8mb4", "utf8mb4_slovenian_ci", "UTF-8"),
		new Encoding(229, "utf8mb4", "utf8mb4_polish_ci", "UTF-8"),
		new Encoding(230, "utf8mb4", "utf8mb4_estonian_ci", "UTF-8"),
		new Encoding(231, "utf8mb4", "utf8mb4_spanish_ci", "UTF-8"),
		new Encoding(232, "utf8mb4", "utf8mb4_swedish_ci", "UTF-8"),
		new Encoding(233, "utf8mb4", "utf8mb4_turkish_ci", "UTF-8"),
		new Encoding(234, "utf8mb4", "utf8mb4_czech_ci", "UTF-8"),
		new Encoding(235, "utf8mb4", "utf8mb4_danish_ci", "UTF-8"),
		new Encoding(236, "utf8mb4", "utf8mb4_lithuanian_ci", "UTF-8"),
		new Encoding(237, "utf8mb4", "utf8mb4_slovak_ci", "UTF-8"),
		new Encoding(238, "utf8mb4", "utf8mb4_spanish2_ci", "UTF-8"),
		new Encoding(239, "utf8mb4", "utf8mb4_roman_ci", "UTF-8"),
		new Encoding(240, "utf8mb4", "utf8mb4_persian_ci", "UTF-8"),
		new Encoding(241, "utf8mb4", "utf8mb4_esperanto_ci", "UTF-8"),
		new Encoding(242, "utf8mb4", "utf8mb4_hungarian_ci", "UTF-8"),
		new Encoding(243, "utf8mb4", "utf8mb4_sinhala_ci", "UTF-8"),
		new Encoding(244, "utf8mb4", "utf8mb4_german2_ci", "UTF-8"),
		new Encoding(245, "utf8mb4", "utf8mb4_croatian_ci", "UTF-8"),
		new Encoding(246, "utf8mb4", "utf8mb4_unicode_520_ci", "UTF-8"),
		new Encoding(247, "utf8mb4", "utf8mb4_vietnamese_ci", "UTF-8"),
		new Encoding(248, "gb18030", "gb18030_chinese_ci", "GB18030"),
		new Encoding(249, "gb18030", "gb18030_bin", "GB18030"),
		new Encoding(250, "gb18030", "gb18030_unicode_520_ci", "GB18030")
	};
}
