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

package udentric.mysql.classic.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPromise;
import udentric.mysql.classic.ProtocolHandler;
import udentric.mysql.classic.ResponseConsumer;

public abstract class Any {
	public abstract void encode(ByteBuf dst, ProtocolHandler ph);
}
