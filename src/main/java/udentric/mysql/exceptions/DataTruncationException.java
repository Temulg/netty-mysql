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
package udentric.mysql.exceptions;

public class DataTruncationException extends CJException {
	public DataTruncationException() {
	}

	public DataTruncationException(String message) {
		super(message);
	}

	public DataTruncationException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataTruncationException(Throwable cause) {
		super(cause);
	}

	protected DataTruncationException(
		String message, Throwable cause, boolean enableSuppression,
		boolean writableStackTrace
	) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DataTruncationException(
		String message, int index, boolean parameter, boolean read,
		int dataSize, int transferSize, int vendorErrorCode
	) {
		super(message);
		this.setIndex(index);
		this.setParameter(parameter);
		this.setRead(read);
		this.setDataSize(dataSize);
		this.setTransferSize(transferSize);
		setVendorCode(vendorErrorCode);
	}

	public int getIndex() {
		return this.index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public boolean isParameter() {
		return this.parameter;
	}

	public void setParameter(boolean parameter) {
		this.parameter = parameter;
	}

	public boolean isRead() {
		return this.read;
	}

	public void setRead(boolean read) {
		this.read = read;
	}

	public int getDataSize() {
		return this.dataSize;
	}

	public void setDataSize(int dataSize) {
		this.dataSize = dataSize;
	}

	public int getTransferSize() {
		return this.transferSize;
	}

	public void setTransferSize(int transferSize) {
		this.transferSize = transferSize;
	}

	private static final long serialVersionUID = 0x2b055a25cb8b784aL;

	private int index;
	private boolean parameter;
	private boolean read;
	private int dataSize;
	private int transferSize;
}
