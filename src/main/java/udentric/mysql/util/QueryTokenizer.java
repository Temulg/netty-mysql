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

package udentric.mysql.util;

public class QueryTokenizer {
	public QueryTokenizer(String src_) {
		src = src_;
		srcLastPos = src != null ? src.length() : 0;
		srcCurPos = 0;
	}

	protected CharSequence updateSubExpr(CharSequence seq) {
		return seq;
	}

	protected void parseSrc() {
		while (srcCurPos < srcLastPos) {
			char ch = src.charAt(srcCurPos);

			switch (curExpr.state) {
			case SUBEXPR:
			case NORMAL:
				switch (ch) {
				case CHR_ESCAPE:
					beginSimpleExpr(State.ESCAPE);
					break;
				case CHR_SGL_QUOTE:
					curExpr.append(ch);
					beginSimpleExpr(State.QUOTE_SGL);
					break;
				case CHR_DBL_QUOTE:
					curExpr.append(ch);
					beginSimpleExpr(State.QUOTE_DBL);
					break;
				case CHR_COMMENT:
					curExpr.append(ch);
					beginSimpleExpr(State.POSSIBLE_COMMENT);
					break;
				case CHR_BEGIN_SUBEXPR:
					beginSubExpr(State.SUBEXPR);
					curExpr.append(ch);
					break;
				case CHR_END_SUBEXPR:
					curExpr.append(ch);
					endExpr();
					break;
				case CHR_VARIABLE:
					curExpr.append(ch);
					hasVariables = true;
					break;
				default:
					curExpr.append(ch);
				}
				break;
			case ESCAPE:
				curExpr.append(ch);
				endExpr();
				break;
			case QUOTE_SGL:
				switch (ch) {
				case CHR_ESCAPE:
					beginSimpleExpr(State.ESCAPE);
					break;
				case CHR_SGL_QUOTE:
					curExpr.append(ch);
					endExpr();
					break;
				default:
					curExpr.append(ch);
				}
				break;
			case QUOTE_DBL:
				switch (ch) {
				case CHR_ESCAPE:
					beginSimpleExpr(State.ESCAPE);
					break;
				case CHR_DBL_QUOTE:
					curExpr.append(ch);
					endExpr();
					break;
				default:
					curExpr.append(ch);
				}
				break;
			case POSSIBLE_COMMENT:
				if (ch != CHR_COMMENT) {
					endExpr();
					continue;
				}

				curExpr.state = State.IN_COMMENT;
				curExpr.append(ch);
				break;
			case IN_COMMENT:
				if (ch == CHR_LF || ch == CHR_CR) {
					curExpr.state = State.AFTER_COMMENT;
				}
				curExpr.append(ch);
				break;
			case AFTER_COMMENT:
				if (ch != CHR_LF && ch != CHR_CR) {
					endExpr();
					continue;
				}
				curExpr.append(ch);
				break;
			}

			srcCurPos++;
		}
	}

	static final char CHR_ESCAPE = '\\';
	static final char CHR_SGL_QUOTE = '\'';
	static final char CHR_DBL_QUOTE = '"';
	static final char CHR_LF = '\n';
	static final char CHR_CR = '\r';
	static final char CHR_COMMENT = '-';
	static final char CHR_BEGIN_SUBEXPR = '{';
	static final char CHR_END_SUBEXPR = '}';
	static final char CHR_VARIABLE = '@';

	protected enum State {
		SUBEXPR,
		NORMAL,
		ESCAPE,
		QUOTE_SGL,
		QUOTE_DBL,
		POSSIBLE_COMMENT,
		IN_COMMENT,
		AFTER_COMMENT;
	}


	protected static abstract class Expr {
		Expr(Expr prev_, State state_) {
			prev = prev_;
			state = state_;
		}

		abstract void append(char ch);
		abstract StringBuilder getBuf();

		final Expr prev;
		State state;
	}

	protected void beginSimpleExpr(State state) {
		curExpr = new SimpleExpr(curExpr, state);
	}

	protected void beginSubExpr(State state) {
		curExpr = new SubExpr(curExpr, state);
	}

	protected void endExpr() {
		if (curExpr instanceof SubExpr) {
			curExpr.prev.getBuf().append(
				updateSubExpr(curExpr.getBuf())
			);
		}

		curExpr = curExpr.prev;
	}

	protected static class SimpleExpr extends Expr {
		SimpleExpr(Expr prev_, State state_) {
			super(prev_, state_);
		}

		@Override
		void append(char ch) {
			prev.append(ch);
		}

		@Override
		StringBuilder getBuf() {
			return prev.getBuf();
		}
	}

	protected static class SubExpr extends Expr {
		SubExpr(Expr prev_, State state_) {
			super(prev_, state_);
		}

		@Override
		void append(char ch) {
			buf.append(ch);
		}

		@Override
		StringBuilder getBuf() {
			return buf;
		}

		final StringBuilder buf = new StringBuilder();
	}

	protected final String src;
	protected final int srcLastPos;
	protected final SubExpr rootExpr = new SubExpr(null, State.NORMAL);
	protected Expr curExpr = rootExpr;
	protected int srcCurPos;
	protected boolean hasVariables = false;
}
