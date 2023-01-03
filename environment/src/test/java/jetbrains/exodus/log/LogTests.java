/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;

import static java.lang.Integer.valueOf;

@SuppressWarnings({"MultiplyOrDivideByPowerOfTwo", "MethodCallInLoopCondition", "AutoUnboxing"})
public class LogTests extends LogTestsBase {

    private static final Loggable DUMMY_LOGGABLE = createNoDataLoggable((byte) 1);
    private static final Loggable ONE_KB_LOGGABLE = createOneKbLoggable();

    @Test
    @Ignore
    public void testWrite() {
        initLog(6, 1024); // file size must be multiple of 3 to avoid alignment
        final long logFileSizeInBytes = adjustedLogFileSize(getLog().getFileLengthBound(), 1024);
        for (int i = 0; i < 100; ++i) {
            log.beginWrite();
            for (int j = 0; j < logFileSizeInBytes; ++j) {
                Assert.assertEquals(expectedAddress(3 * (j + i * logFileSizeInBytes), 1024),
                        getLog().write(DUMMY_LOGGABLE));
            }
            log.endWrite();

            Assert.assertEquals(3 * (i + 1), (int) getLog().getNumberOfFiles()); // each DUMMY_LOGGABLE takes 3 bytes
        }
    }

    @Test
    public void testHighestPage() {
        initLog(1, 1024);

        final Loggable emptyLoggable = NullLoggable.create();

        getLog().beginWrite();
        for (int i = 0; i < 1024; ++i) {
            getLog().write(emptyLoggable);
        }
        getLog().flush();
        getLog().endWrite();
        closeLog();

        initLog(1, 1024);
    }

    @Test
    @Ignore
    public void testWrite2() {
        initLog(111, 1024); // file size must be multiple of 3 to avoid alignment

        getLog().beginWrite();
        for (int j = 0; j < adjustedLogFileSize(111 * 1024, 1024); ++j) {
            Assert.assertEquals(expectedAddress(3L * j, 1024), (int) getLog().write(DUMMY_LOGGABLE));
        }

        getLog().flush();
        getLog().endWrite();
        Assert.assertEquals(3, (int) getLog().getNumberOfFiles()); // each DUMMY_LOGGABLE takes 3 bytes
    }

    @Test
    public void testRemoveFile() {
        initLog(1, 1024);
        getLog().beginWrite();
        for (int j = 0; j < adjustedLogFileSize(1024, 1024) * 99; ++j) {
            getLog().write(NullLoggable.create());
        }
        getLog().flush();

        log.forgetFiles(new long[]{0, 1024, 8192, 32768});
        getLog().endWrite();
        getLog().removeFile(0);
        getLog().removeFile(1024);
        getLog().removeFile(8192);
        getLog().removeFile(32768);
        Assert.assertEquals(95, (int) getLog().getNumberOfFiles()); // null loggable should take only one byte
    }

    @Test
    public void testRemoveFileInvalidAddress() {
        initLog(1, 1024);
        getLog().beginWrite();
        for (int j = 0; j < adjustedLogFileSize(1024, 1024) * 10; ++j) {
            getLog().write(NullLoggable.create());
        }
        getLog().flush();
        getLog().endWrite();
        TestUtil.runWithExpectedException(() -> getLog().removeFile(1111), ExodusException.class);
    }

    @Test
    public void testRemoveFileNonexistentAddress() {
        initLog(1, 1024);
        getLog().beginWrite();
        for (int j = 0; j < adjustedLogFileSize(1024, 1024) * 10; ++j) {
            getLog().write(NullLoggable.create());
        }
        getLog().flush();
        getLog().endWrite();
        TestUtil.runWithExpectedException(() -> getLog().removeFile(1024 * 10), ExodusException.class);
    }

    @Test
    public void testPaddingWithNulls() {
        initLog(1, 1024);
        getLog().beginWrite();
        for (int i = 0; i < 100; ++i) {
            getLog().write(DUMMY_LOGGABLE);
            getLog().doPadWithNulls();
            Assert.assertEquals(i + 1, getLog().getWrittenFilesSize());
        }
        getLog().flush();
        getLog().endWrite();
    }

    @Test
    public void testWriteNulls() {
        initLog(1, 1024);
        log.beginWrite();
        for (int j = 0; j < adjustedLogFileSize(1024, 1024) * 99; ++j) {
            getLog().write(NullLoggable.create());
        }
        getLog().flush();
        log.endWrite();
        Assert.assertEquals(99, (int) getLog().getNumberOfFiles()); // null loggable should take only one byte
    }

    @Test
    public void testAutoAlignment() {
        initLog(1, 1024);
        log.beginWrite();
        for (int i = 0; i < adjustedLogFileSize(1024, 1024) - 1; ++i) {
            getLog().write(NullLoggable.create());
        }
        // here auto-alignment should happen
        final long dummyAddress = getLog().write(DUMMY_LOGGABLE);
        getLog().flush();
        getLog().endWrite();
        Assert.assertEquals(2, (int) getLog().getNumberOfFiles());
        // despite the fact that DUMMY_LOGGABLE size + 1023 nulls should result in 1025 bytes,
        // we should actually get 1026 (one kb + 2) due to automatic alignment
        Assert.assertEquals(1027, (int) getLog().getHighAddress());
        // by the same reason, address of dummy should be at the beginning of the second file
        Assert.assertEquals(1024, (int) dummyAddress);
    }

    @Test
    public void testWriteReadSameAddress() {
        initLog(1, 1024);
        for (int i = 0; i < 100; ++i) {
            getLog().beginWrite();
            final long dummyAddress = getLog().write(DUMMY_LOGGABLE);
            getLog().flush();
            getLog().endWrite();
            Assert.assertEquals(expectedAddress(i * 3L, 1024), dummyAddress);
            Assert.assertEquals(dummyAddress, getLog().read(dummyAddress).getAddress());
        }
    }

    @Test
    public void testAutoAlignment2() {
        initLog(1, 1024);
        // one kb loggable can't be placed in a single file of one kb size
        TestUtil.runWithExpectedException(() -> {
            log.beginWrite();
            getLog().write(ONE_KB_LOGGABLE);
        }, TooBigLoggableException.class);
    }

    @Test
    public void testReadUnknownLoggableType() {
        getLog().beginWrite();
        getLog().write(DUMMY_LOGGABLE);
        getLog().flush();
        getLog().endWrite();
        getLog().read(0);
    }

    @Test
    public void testWriteImmediateRead() {
        testWriteImmediateRead(1, 1024);
    }

    @Test
    public void testWriteImmediateRead2() {
        testWriteImmediateRead(4, 1024 * 4);
    }

    @Test
    public void testWriteImmediateRead3() {
        testWriteImmediateRead(16, 1024 * 16);
    }

    @Test
    public void testWriteImmediateRead4() {
        testWriteImmediateRead(2, 1024);
    }

    @Test
    public void testWriteSequentialRead() {
        testWriteSequentialRead(1, 1024);
    }

    @Test
    public void testWriteSequentialRead2() {
        testWriteSequentialRead(4, 1024 * 4);
    }

    @Test
    public void testWriteSequentialRead3() {
        testWriteSequentialRead(16, 1024 * 16);
    }

    @Test
    public void testWriteSequentialRead4() {
        testWriteSequentialRead(2, 1024);
    }

    @Test
    public void testWriteRandomRead() {
        testWriteRandomRead(1, 1024);
    }

    @Test
    public void testWriteRandomRead2() {
        testWriteRandomRead(4, 1024 * 4);
    }

    @Test
    public void testWriteRandomRead3() {
        testWriteRandomRead(16, 1024 * 16);
    }

    @Test
    public void testWriteRandomRead4() {
        testWriteRandomRead(2, 1024);
    }

    @Test
    public void testAllLoggablesIterator() {
        initLog(4, 1024 * 4);
        final int count = 10;
        getLog().beginWrite();
        for (int i = 0; i < count; ++i) {
            writeData(CompressedUnsignedLongByteIterable.getIterable(i));
        }
        getLog().flush();
        getLog().endWrite();
        final Iterator<RandomAccessLoggable> it = getLog().getLoggableIterator(0);
        int i = 0;
        while (it.hasNext()) {
            Loggable l = it.next();
            assert l != null;

            Assert.assertEquals(4 * i++, l.getAddress());
            Assert.assertEquals(127, l.getType());
            Assert.assertEquals(1, l.getDataLength());
        }
        Assert.assertEquals(count, i);
    }

    private long writeData(ByteIterable iterable) {
        return getLog().write((byte) 127, Loggable.NO_STRUCTURE_ID, iterable);
    }

    @Test
    public void testAllRandomAccessLoggablesIterator() {
        initLog(4, 1024 * 4);
        final int count = 10;
        getLog().beginWrite();
        for (int i = 0; i < count; ++i) {
            writeData(CompressedUnsignedLongByteIterable.getIterable(i));
        }
        getLog().flush();
        getLog().endWrite();
        final Iterator<RandomAccessLoggable> it = getLog().getLoggableIterator(0);
        int i = 0;
        while (it.hasNext()) {
            Loggable l = it.next();
            assert l != null;
            Assert.assertEquals(4 * i++, l.getAddress());
            Assert.assertEquals(127, l.getType());
            Assert.assertEquals(1, l.getDataLength());
        }
        Assert.assertEquals(count, i);
    }

    private void testWriteImmediateRead(int fileSize, int pageSize) {
        initLog(fileSize, pageSize);
        final int count = 50000;
        for (int i = 0; i < count; ++i) {
            log.beginWrite();
            final long addr = writeData(CompressedUnsignedLongByteIterable.getIterable(i));
            getLog().flush();
            log.endWrite();
            Assert.assertEquals(i, (int) CompressedUnsignedLongByteIterable.getLong(getLog().read(addr).getData()));
        }
    }

    private void testWriteSequentialRead(int fileSize, int pageSize) {
        initLog(fileSize, pageSize);
        final int count = 50000;
        final LongArrayList addrs = new LongArrayList();
        log.beginWrite();
        for (int i = 0; i < count; ++i) {
            addrs.add(writeData(CompressedUnsignedLongByteIterable.getIterable(i)));
        }
        log.flush();
        log.endWrite();
        for (int i = 0; i < count; ++i) {
            Assert.assertEquals(i, (int) CompressedUnsignedLongByteIterable.getLong(getLog().read(addrs.get(i)).getData()));
        }
    }

    private void testWriteRandomRead(int fileSize, int pageSize) {
        initLog(fileSize, pageSize);
        final int count = 50000;
        final LongHashMap<Integer> addrs = new LongHashMap<>();
        log.beginWrite();
        for (int i = 0; i < count; ++i) {
            final long addr = writeData(CompressedUnsignedLongByteIterable.getIterable(i));
            addrs.put(addr, valueOf(i));
        }
        log.flush();
        log.endWrite();

        for (Long addr : addrs.keySet()) {
            Assert.assertEquals((int) addrs.get(addr),
                    (int) CompressedUnsignedLongByteIterable.getLong(getLog().read(addr).getData()));
            getLog().read(addr);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static TestLoggable createNoDataLoggable(byte type) {
        return new TestLoggable(type, ByteIterable.EMPTY, Loggable.NO_STRUCTURE_ID);
    }

    @SuppressWarnings("SameParameterValue")
    static long expectedAddress(long written, int pageSize) {
        int adjustedPageSize = pageSize - BufferedDataWriter.LOGGABLE_DATA;
        long reminder = written % adjustedPageSize;
        long pages = (written - reminder) / adjustedPageSize;

        return pages * pageSize + reminder;
    }

    @SuppressWarnings("SameParameterValue")
    public static long adjustedLogFileSize(long fileSize, int pageSize) {
        long pages = fileSize / pageSize;
        return (pageSize - BufferedDataWriter.LOGGABLE_DATA) * pages;
    }
}
