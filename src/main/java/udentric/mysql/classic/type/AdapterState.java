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

import io.netty.buffer.ByteBufAllocator;
import java.util.function.Consumer;

public class AdapterState {
	public AdapterState(ByteBufAllocator alloc_) {
		alloc = alloc_;
	}

	/*-- Adapter access --*/
	public void markAsDone() {
		done = true;
	}

	@SuppressWarnings("unchecked") 
	public <T> T get() {
		return (T)state;
	}

	public <T> void set(T state_) {
		state = state_;
	}

	public <T> void set(T state_, Consumer<T> release_) {
		state = state_;
		release = release_;
	}

	/*-- Controller access --*/

	public boolean done() {
		return done;
	}

	@SuppressWarnings("unchecked")
	public void reset() {
		release.accept(state);
		state = null;
		release = AdapterState::defaultStateRelease;
		done = false;
	}

	private static void defaultStateRelease(Object obj) {
	}

	private Object state;
	private Consumer release = AdapterState::defaultStateRelease;
	private boolean done;
	public final ByteBufAllocator alloc;
}
