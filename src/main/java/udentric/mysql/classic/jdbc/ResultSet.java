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

package udentric.mysql.classic.jdbc;

import io.netty.channel.ChannelPromise;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.util.Calendar;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import udentric.mysql.classic.ColumnDefinition;
import udentric.mysql.classic.Packet;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.Row;

public class ResultSet implements java.sql.ResultSet, ResultSetConsumer {
	ResultSet(Statement stmt_) {
		stmt = stmt_;
		resultSetActive = stmt.getConnection().getChannel().newPromise();
	}

	@Override
	public boolean next() throws SQLException {
		return false;
	}

	@Override
	public void close() throws SQLException {
		stmt.releaseResult(this);
	}

	@Override
	public  boolean wasNull() throws SQLException {
		return false;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return false;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return 0;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return 0;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return 0;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return 0;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return 0;
	}

	@Deprecated
	@Override
	public BigDecimal getBigDecimal(
		int columnIndex, int scale
	) throws SQLException {
		return null;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Date getDate(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Time getTime(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Timestamp getTimestamp(
		int columnIndex
	) throws SQLException {
		return null;
	}

	@Override
	public InputStream getAsciiStream(
		int columnIndex
	) throws SQLException {
		return null;
	}

	@Deprecated
	@Override
	public InputStream getUnicodeStream(
		int columnIndex
	) throws SQLException {
		return null;
	}

	@Override
	public InputStream getBinaryStream(
		int columnIndex
	) throws SQLException {
		return null;
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return false;
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return 0;
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return 0;
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return 0;
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return 0;
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return 0;
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return 0;
	}

	@Deprecated
	@Override
	public BigDecimal getBigDecimal(
		String columnLabel, int scale
	) throws SQLException {
		return null;
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Date getDate(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Time getTime(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Timestamp getTimestamp(
		String columnLabel
	) throws SQLException {
		return null;
	}

	@Override
	public InputStream getAsciiStream(
		String columnLabel
	) throws SQLException {
		return null;
	}

	@Deprecated
	@Override
	public InputStream getUnicodeStream(
		String columnLabel
	) throws SQLException {
		return null;
	}

	@Override
	public InputStream getBinaryStream(
		String columnLabel
	) throws SQLException {
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public String getCursorName() throws SQLException {
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return null;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return 0;
	}

	@Override
	public Reader getCharacterStream(
		int columnIndex
	) throws SQLException {
		return null;
	}

	@Override
	public Reader getCharacterStream(
		String columnLabel
	) throws SQLException {
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(
		String columnLabel
	) throws SQLException {
		return null;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return false;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		return false;
	}

	@Override
	public void beforeFirst() throws SQLException {
	}

	@Override
	public void afterLast() throws SQLException {
	}

	@Override
	public boolean first() throws SQLException {
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		return false;
	}

	@Override
	public int getRow() throws SQLException	{
		return 0;
	}

	@Override
	public boolean absolute( int row ) throws SQLException {
		return false;
	}

	@Override
	public boolean relative( int rows ) throws SQLException {
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {		
	}

	@Override
	public int getFetchSize() throws SQLException {
		return 0;
	}

	@Override
	public int getType() throws SQLException {
		return 0;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return 0;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
	}

	@Override
	public void updateBoolean(
		int columnIndex, boolean x
	) throws SQLException {
	}


	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
	}


	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
	}


	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
	}


	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
	}


	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
	}


	@Override
	public void updateDouble(
		int columnIndex, double x
	) throws SQLException {
	}


	@Override
	public void updateBigDecimal(
		int columnIndex, BigDecimal x
	) throws SQLException {
	}

	@Override
	public void updateString(
		int columnIndex, String x
	) throws SQLException {
	}


	@Override
	public void updateBytes(
		int columnIndex, byte x[]
	) throws SQLException {
	}


	@Override
	public void updateDate(
		int columnIndex, java.sql.Date x
	) throws SQLException {
	}

	@Override
	public void updateTime(
		int columnIndex, java.sql.Time x
	) throws SQLException {
	}


	@Override
	public void updateTimestamp(
		int columnIndex, java.sql.Timestamp x
	) throws SQLException {
	}


	@Override
	public void updateAsciiStream(
		int columnIndex, InputStream x, int length
	) throws SQLException {
	}

	@Override
	public void updateBinaryStream(
		int columnIndex, InputStream x, int length
	) throws SQLException {
	}

	@Override
	public void updateCharacterStream(
		int columnIndex, Reader x, int length
	) throws SQLException {
	}

	@Override
	public void updateObject(
		int columnIndex, Object x, int scaleOrLength
	) throws SQLException {
	}

	@Override
	public void updateObject(
		int columnIndex, Object x
	) throws SQLException {
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
	}

	@Override
	public void updateBoolean(
		String columnLabel, boolean x
	) throws SQLException {
	}

	@Override
	public void updateByte(
		String columnLabel, byte x
	) throws SQLException {
	}

	@Override
	public void updateShort(
		String columnLabel, short x
	) throws SQLException {
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
	}

	@Override
	public void updateFloat(
		String columnLabel, float x
	) throws SQLException {
	}

	@Override
	public void updateDouble(
		String columnLabel, double x
	) throws SQLException {
	}

	@Override
	public void updateBigDecimal(
		String columnLabel, BigDecimal x
	) throws SQLException {
	}

	@Override
	public void updateString(
		String columnLabel, String x
	) throws SQLException {
	}

	@Override
	public void updateBytes(
		String columnLabel, byte x[]
	) throws SQLException {
	}

	@Override
	public void updateDate(
		String columnLabel, java.sql.Date x
	) throws SQLException {
	}

	@Override
	public void updateTime(
		String columnLabel, java.sql.Time x
	) throws SQLException {
	}

	@Override
	public void updateTimestamp(
		String columnLabel, java.sql.Timestamp x
	) throws SQLException {
	}

	@Override
	public void updateAsciiStream(
		String columnLabel, InputStream x, int length
	) throws SQLException {
	}

	@Override
	public void updateBinaryStream(
		String columnLabel, InputStream x, int length
	) throws SQLException {
	}

	@Override
	public void updateCharacterStream(
		String columnLabel, Reader reader, int length
	) throws SQLException {
	}

	@Override
	public void updateObject(
		String columnLabel, Object x, int scaleOrLength
	) throws SQLException {
	}

	@Override
	public void updateObject(
		String columnLabel, Object x
	) throws SQLException {
	}

	@Override
	public void insertRow() throws SQLException {
	}

	@Override
	public void updateRow() throws SQLException {
	}

	@Override
	public void deleteRow() throws SQLException {
	}

	@Override
	public void refreshRow() throws SQLException {
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
	}

	@Override
	public void moveToInsertRow() throws SQLException {
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
	}

	@Override
	public Statement getStatement() throws SQLException {
		return stmt;
	}

	@Override
	public Object getObject(
		int columnIndex, java.util.Map<String,Class<?>> map
	) throws SQLException {
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public Object getObject(
		String columnLabel, java.util.Map<String,Class<?>> map
	) throws SQLException {
		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Date getDate(
		int columnIndex, Calendar cal
	) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Date getDate(
		String columnLabel, Calendar cal
	) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Time getTime(
		int columnIndex, Calendar cal
	) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Time getTime(
		String columnLabel, Calendar cal
	) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Timestamp getTimestamp(
		int columnIndex, Calendar cal
	) throws SQLException {
		return null;
	}

	@Override
	public java.sql.Timestamp getTimestamp(
		String columnLabel, Calendar cal
	) throws SQLException {
		return null;
	}

	@Override
	public java.net.URL getURL(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public java.net.URL getURL(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public void updateRef(
		int columnIndex, java.sql.Ref x
	) throws SQLException {
	}

	@Override
	public void updateRef(
		String columnLabel, java.sql.Ref x
	) throws SQLException {
	}

	@Override
	public void updateBlob(
		int columnIndex, java.sql.Blob x
	) throws SQLException {
	}

	@Override
	public void updateBlob(
		String columnLabel, java.sql.Blob x
	) throws SQLException {
	}

	@Override
	public void updateClob(
		int columnIndex, java.sql.Clob x
	) throws SQLException {
	}

	@Override
	public void updateClob(
		String columnLabel, java.sql.Clob x
	) throws SQLException {
	}

	@Override
	public void updateArray(
		int columnIndex, java.sql.Array x
	) throws SQLException {
	}

	@Override
	public void updateArray(
		String columnLabel, java.sql.Array x
	) throws SQLException {
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
	}

	@Override
	public void updateRowId(
		String columnLabel, RowId x
	) throws SQLException {
	}

	@Override
	public int getHoldability() throws SQLException {
		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public void updateNString(
		int columnIndex, String nString
	) throws SQLException {
	}

	@Override
	public void updateNString(
		String columnLabel, String nString
	) throws SQLException {
	}

	@Override
	public void updateNClob(
		int columnIndex, NClob nClob
	) throws SQLException {
	}

	@Override
	public void updateNClob(
		String columnLabel, NClob nClob
	) throws SQLException {
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public void updateSQLXML(
		int columnIndex, SQLXML xmlObject
	) throws SQLException {
	}

	@Override
	public void updateSQLXML(
		String columnLabel, SQLXML xmlObject
	) throws SQLException {
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return null;
	}

	@Override
	public Reader getNCharacterStream(
		int columnIndex
	) throws SQLException {
		return null;
	}

	@Override
	public Reader getNCharacterStream(
		String columnLabel
	) throws SQLException {
		return null;
	}

	@Override
	public void updateNCharacterStream(
		int columnIndex, Reader x, long length
	) throws SQLException {
	}

	@Override
	public void updateNCharacterStream(
		String columnLabel, Reader reader, long length
	) throws SQLException {
	}

	@Override
	public void updateAsciiStream(
		int columnIndex, InputStream x, long length
	) throws SQLException {
	}

	@Override
	public void updateBinaryStream(
		int columnIndex, InputStream x, long length
	) throws SQLException {
	}

	@Override
	public void updateCharacterStream(
		int columnIndex, Reader x, long length
	) throws SQLException {
	}

	@Override
	public void updateAsciiStream(
		String columnLabel, InputStream x, long length
	) throws SQLException {
	}

	@Override
	public void updateBinaryStream(
		String columnLabel, InputStream x, long length
	) throws SQLException {
	}

	@Override
	public void updateCharacterStream(
		String columnLabel, Reader reader, long length
	) throws SQLException {
	}

	@Override
	public void updateBlob(
		int columnIndex, InputStream inputStream, long length
	) throws SQLException {
	}

	@Override
	public void updateBlob(
		String columnLabel, InputStream inputStream, long length
	) throws SQLException {
	}

	@Override
	public void updateClob(
		int columnIndex, Reader reader, long length
	) throws SQLException {
	}

	@Override
	public void updateClob(
		String columnLabel, Reader reader, long length
	) throws SQLException {
	}

	@Override
	public void updateNClob(
		int columnIndex, Reader reader, long length
	) throws SQLException {
	}

	@Override
	public void updateNClob(
		String columnLabel, Reader reader, long length
	) throws SQLException {
	}

	@Override
	public void updateNCharacterStream(
		int columnIndex, Reader x
	) throws SQLException {
	}

	@Override
	public void updateNCharacterStream(
		String columnLabel, Reader reader
	) throws SQLException {
	}

	@Override
	public void updateAsciiStream(
		int columnIndex, InputStream x
	) throws SQLException {
	}

	@Override
	public void updateBinaryStream(
		int columnIndex, InputStream x
	) throws SQLException {
	}

	@Override
	public void updateCharacterStream(
		int columnIndex, Reader x
	) throws SQLException {
	}

	@Override
	public void updateAsciiStream(
		String columnLabel, InputStream x
	) throws SQLException {
	}

	@Override
	public void updateBinaryStream(
		String columnLabel, InputStream x
	) throws SQLException {
	}

	@Override
	public void updateCharacterStream(
		String columnLabel, Reader reader
	) throws SQLException {
	}

	@Override
	public void updateBlob(
		int columnIndex, InputStream inputStream
	) throws SQLException {
	}

	@Override
	public void updateBlob(
		String columnLabel, InputStream inputStream
	) throws SQLException {
	}

	@Override
	public void updateClob(
		int columnIndex, Reader reader
	) throws SQLException {
	}

	@Override
	public void updateClob(
		String columnLabel, Reader reader
	) throws SQLException {
	}

	@Override
	public void updateNClob(
		int columnIndex, Reader reader
	) throws SQLException {
	}

	@Override
	public void updateNClob(
		String columnLabel, Reader reader
	) throws SQLException {
	}

	@Override
	public <T> T getObject(
		int columnIndex, Class<T> type
	) throws SQLException {
		return null;
	}

	@Override
	public <T> T getObject(
		String columnLabel, Class<T> type
	) throws SQLException {
		return null;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public void acceptMetadata(ColumnDefinition colDef_) {
		colDef = colDef_;
		resultSetActive.setSuccess();
	}

	@Override
	public void acceptRow(Row row) {
		incomingRows.offer(row);
		cond.signal();
	}

	@Override
	public void onFailure(Throwable cause) {
		if (colDef == null)
			resultSetActive.setFailure(cause);
		else {
			error = cause;
			cond.signalAll();
		}
	}

	@Override
	public void onSuccess(Packet.ServerAck ack) {
		expectMoreRows = false;
		cond.signalAll();
	}

	private final Statement stmt;
	private final LinkedTransferQueue<
		Row
	> incomingRows = new LinkedTransferQueue<>();
	final ChannelPromise resultSetActive;
	private volatile ColumnDefinition colDef;

	private final ReentrantLock lock = new ReentrantLock();
	private final Condition cond = lock.newCondition();
	private volatile Throwable error;
	private volatile boolean expectMoreRows;
	private Row current;
}
