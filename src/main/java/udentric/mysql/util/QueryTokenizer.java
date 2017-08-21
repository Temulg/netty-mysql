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
package udentric.mysql.util;

import java.util.ArrayDeque;
import java.util.ArrayList;

public class QueryTokenizer {
	public QueryTokenizer(String s_) {
		s = s_;
		last = s.length();
		cur = 0;
		state.offerLast(State.NORMAL);
	}

	public boolean hasMoreTokens() {
		return cur < last;
	}

	public CharSequence nextToken() {
		tokenBuf.setLength(0);

		tokenLoop: while (cur < last) {
			char ch = s.charAt(cur);

			switch (state.peekLast()) {
			case NORMAL:
				switch (ch) {
				case CHR_ESCAPE:
					state.offerLast(State.ESCAPE);
					break;
				case CHR_SGL_QUOTE:
					tokenBuf.append(ch);
					state.offerLast(State.QUOTE_SGL);
					break;
				case CHR_DBL_QUOTE:
					tokenBuf.append(ch);
					state.offerLast(State.QUOTE_DBL);
					break;
				case CHR_COMMENT:
					tokenBuf.append(ch);
					state.offerLast(State.POSSIBLE_COMMENT);
					break;
				case CHR_BEGIN_TOKEN:
					
					break;
				case CHR_VARIABLE:
					tokenBuf.append(ch);
					hasVariables = true;
					break;
				default:
					tokenBuf.append(ch);
				}
				break;
			case ESCAPE:
				tokenBuf.append(ch);
				state.pollLast();
				break;
			case QUOTE_SGL:
				switch (ch) {
				case CHR_ESCAPE:
					state.offerLast(State.ESCAPE);
					break;
				case CHR_SGL_QUOTE:
					tokenBuf.append(ch);
					state.pollLast();
					break;
				default:
					tokenBuf.append(ch);
				}
				break;
			case QUOTE_DBL:
				switch (ch) {
				case CHR_ESCAPE:
					state.offerLast(State.ESCAPE);
					break;
				case CHR_DBL_QUOTE:
					tokenBuf.append(ch);
					state.pollLast();
					break;
				default:
					tokenBuf.append(ch);
				}
				break;
			case POSSIBLE_COMMENT:
				state.pollLast();
				if (ch != CHR_COMMENT)
					continue;

				state.offerLast(State.IN_COMMENT);
				tokenBuf.append(ch);
				break;
			case IN_COMMENT:
				if (ch == CHR_LF || ch == CHR_CR) {
					state.pollLast();
					state.offerLast(State.AFTER_COMMENT);
				}
				tokenBuf.append(ch);
				break;
			case AFTER_COMMENT:
				if (ch != CHR_LF && ch != CHR_CR) {
					state.pollLast();
					continue;
				}
				tokenBuf.append(ch);
				break;
			}

			cur++;
		}

		return tokenBuf;
	}

	private static final char CHR_ESCAPE = '\\';
	private static final char CHR_SGL_QUOTE = '\'';
	private static final char CHR_DBL_QUOTE = '"';
	private static final char CHR_LF = '\n';
	private static final char CHR_CR = '\r';
	private static final char CHR_COMMENT = '-';
	private static final char CHR_BEGIN_TOKEN = '{';
	private static final char CHR_END_TOKEN = '}';
	private static final char CHR_VARIABLE = '@';

	private enum State {
		NORMAL,
		ESCAPE,
		QUOTE_SGL,
		QUOTE_DBL,
		POSSIBLE_COMMENT,
		IN_COMMENT,
		AFTER_COMMENT;
	}

	private final String s;
	private final ArrayDeque<State> state = new ArrayDeque<>();
	private final int last;
	private final StringBuilder tokenBuf = new StringBuilder();
	private int cur;
	private boolean hasVariables = false;
}
