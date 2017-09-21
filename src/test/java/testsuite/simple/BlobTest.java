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
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.annotations.TestInstance;
import org.testng.log4testng.Logger;

import testsuite.TestCase;

public class BlobTest extends TestCase {
	public BlobTest() {
		super(Logger.getLogger(BlobTest.class));
	}

	@BeforeClass
	public void beforeClass() throws Exception {
		createBlobFile(1 << 25);
		createTable(
			"BLOBTEST",
			"(pos int PRIMARY KEY auto_increment, blobdata LONGBLOB)"
		);
	}

	@Test
	public void byteStreamInsert() throws Exception {
		BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(testBlobFile));
		this.pstmt = c.prepareStatement("INSERT INTO BLOBTEST(blobdata) VALUES (?)");
		this.pstmt.setBinaryStream(1, bIn, (int) testBlobFile.length());
		this.pstmt.execute();

		this.pstmt.clearParameters();
		doRetrieval();
	}

	private boolean checkBlob(byte[] retrBytes) throws Exception {
		boolean passed = false;
		BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(testBlobFile));

		try {
            int fileLength = (int) testBlobFile.length();
            if (retrBytes.length == fileLength) {
                for (int i = 0; i < fileLength; i++) {
                    byte fromFile = (byte) (bIn.read() & 0xff);

                    if (retrBytes[i] != fromFile) {
                        passed = false;
                        System.out.println("Byte pattern differed at position " + i + " , " + retrBytes[i] + " != " + fromFile);

                        for (int j = 0; (j < (i + 10)) /* && (j < i) */; j++) {
                            System.out.print(Integer.toHexString(retrBytes[j] & 0xff) + " ");
                        }

                        break;
                    }

                    passed = true;
                }
            } else {
                passed = false;
                System.out.println("retrBytes.length(" + retrBytes.length + ") != testBlob.length(" + fileLength + ")");
            }

            return passed;
        } finally {
            if (bIn != null) {
                bIn.close();
            }
		}
	}

	private void doRetrieval() throws Exception {
		boolean passed = false;
        this.rs = this.stmt.executeQuery("SELECT blobdata from BLOBTEST LIMIT 1");
        this.rs.next();

        byte[] retrBytes = this.rs.getBytes(1);
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getBytes().", passed);
        retrBytes = this.rs.getBlob(1).getBytes(1L, (int) this.rs.getBlob(1).length());
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getBlob().", passed);

        InputStream inStr = this.rs.getBinaryStream(1);
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        int b;

        while ((b = inStr.read()) != -1) {
            bOut.write((byte) b);
        }

        retrBytes = bOut.toByteArray();
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getBinaryStream().", passed);
        inStr = this.rs.getAsciiStream(1);
        bOut = new ByteArrayOutputStream();

        while ((b = inStr.read()) != -1) {
            bOut.write((byte) b);
        }

        retrBytes = bOut.toByteArray();
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getAsciiStream().", passed);
        inStr = this.rs.getUnicodeStream(1);
        bOut = new ByteArrayOutputStream();

        while ((b = inStr.read()) != -1) {
            bOut.write((byte) b);
        }

        retrBytes = bOut.toByteArray();
        passed = checkBlob(retrBytes);
        assertTrue("Inserted BLOB data did not match retrieved BLOB data for getUnicodeStream().", passed);
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
