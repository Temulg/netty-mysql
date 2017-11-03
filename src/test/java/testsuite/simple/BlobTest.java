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
 * May contain portions of MySQL Connector/J testsuite
 *
 * Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * The MySQL Connector/J is licensed under the terms of the GPLv2
 * <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL
 * Connectors. There are special exceptions to the terms and conditions of
 * the GPLv2 as it is applied to this software, see the FOSS License Exception
 * <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
 */

package testsuite.simple;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedNioFile;
import testsuite.TestCase;
import udentric.mysql.DataRow;
import udentric.mysql.FieldSet;
import udentric.mysql.PreparedStatement;
import udentric.mysql.classic.Channels;
import udentric.mysql.classic.ResultSetConsumer;
import udentric.mysql.classic.ServerAck;
import udentric.mysql.classic.SyncCommands;
import udentric.mysql.classic.dicta.Query;

public class BlobTest extends TestCase {
	public BlobTest() {
		super(Logger.getLogger(BlobTest.class));
	}

	@BeforeClass
	public void beforeClass() throws Exception {
		createBlobFile(1 << 12/*25*/);
		createTable(
			"blobtest",
			"(pos int PRIMARY KEY auto_increment, blobdata LONGBLOB)"
		);
	}

	@Test
	public void byteStreamInsert() throws Exception {
		logger.info("--1-\n");
		PreparedStatement pstmt = SyncCommands.prepareStatement(
			channel(),  "INSERT INTO blobtest(blobdata) VALUES (?)"
		);
		logger.info("--2-\n");
		try (FileChannel fc = FileChannel.open(
			testBlobFile, StandardOpenOption.READ
		)) {
			logger.info("--3-\n");
			SyncCommands.executeUpdate(
				channel(), pstmt, fc
			);
			logger.info("--4-\n");
		}
		logger.info("--5-\n");
		doRetrieval();
	}

	private boolean checkBlob(ByteBuf retrBytes) {
		int pos = 0;
		try (FileChannel fc = FileChannel.open(
			testBlobFile, StandardOpenOption.READ
		)) {
			ChunkedNioFile f = new ChunkedNioFile(fc);
			while (!f.isEndOfInput()) {
				ByteBuf next = f.readChunk(channel().alloc());
				int count = next.readableBytes();
				if (count == 0) {
					next.release();
					continue;
				}

				if (retrBytes.readableBytes() < count) {
					next.release();
					logger.error(String.format(
						"data mismatch after %d bytes",
					pos));
					return false;
				}

				ByteBuf cur = retrBytes.slice(
					retrBytes.readerIndex(),
					next.readableBytes()
				);
				if (0 != cur.compareTo(next)) {
					next.release();
					logger.error(String.format(
						"data mismatch after %d bytes",
					pos));
					return false;
				}

				retrBytes.skipBytes(count);
				next.release();
				pos += count;
			}

			return retrBytes.readableBytes() == 0;
		} catch (Exception e) {
			Channels.throwAny(e);
			return false;
		}
	}

	private void doRetrieval() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);

		channel().writeAndFlush(new Query(
			"SELECT blobdata from BLOBTEST LIMIT 1",
			new ResultSetConsumer(){
				@Override
				public void acceptRow(DataRow row) {
					Assert.assertTrue(checkBlob(
						columns.getValue(
							row, 0, ByteBuf.class
						)
					));
				}

				@Override
				public void acceptMetadata(
					FieldSet columns_
				) {
					columns = columns_;
				}
			
				@Override
				public void acceptFailure(Throwable cause) {
					Assert.fail("query failed", cause);
					latch.countDown();
				}
			
				@Override
				public void acceptAck(
					ServerAck ack, boolean terminal
				) {
					Assert.assertTrue(terminal);
					latch.countDown();
				}

				FieldSet columns;
			}
		)).addListener(Channels::defaultSendListener);
		latch.await();
	}

	private void createBlobFile(int size) throws IOException {
		if (testBlobFile != null) {
			Files.deleteIfExists(testBlobFile);
		}

		testBlobFile = Files.createTempFile(
			TEST_BLOB_FILE_PREFIX, ".dat"
		);

		try (OutputStream out = Files.newOutputStream(
			testBlobFile, StandardOpenOption.WRITE,
			StandardOpenOption.TRUNCATE_EXISTING
		)) {
			byte[] buf = new byte[4096];
			int rem = size;
			ThreadLocalRandom rnd = ThreadLocalRandom.current();

			while (rem > 0) {
				rnd.nextBytes(buf);
				int len = Math.min(rem, buf.length);
				out.write(buf, 0, len);
				rem -= len;
			}
		}
	}

	@AfterClass
	public void afterClass() throws IOException {
		Files.delete(testBlobFile);
	}

	private static final String TEST_BLOB_FILE_PREFIX = "nmql-testblob";

	protected static Path testBlobFile;
}
