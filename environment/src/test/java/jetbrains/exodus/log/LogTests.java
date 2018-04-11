/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import jetbrains.exodus.TestFor;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.Integer.valueOf;

public class LogTests extends LogTestsBase {

    private static final Loggable DUMMY_LOGGABLE = createNoDataLoggable((byte) 1);
    private static final Loggable ONE_KB_LOGGABLE = createOneKbLoggable();

    @Test
    public void testWrite() {
        initLog(6); // file size must be multiple of 3 to avoid alignment
        final long logFileSizeInBytes = getLog().getFileLengthBound();
        for (int i = 0; i < 100; ++i) {
            log.beginWrite();
            for (int j = 0; j < logFileSizeInBytes; ++j) {
                Assert.assertEquals(3 * (j + i * logFileSizeInBytes), getLog().write(DUMMY_LOGGABLE));
            }
            log.endWrite();
            Assert.assertEquals(3 * (i + 1), (int) getLog().getNumberOfFiles()); // each DUMMY_LOGGABLE takes 2 bytes
        }
    }

    @Test
    public void testHighestPage() throws IOException {
        initLog(1);

        final Loggable emptyLoggable = NullLoggable.create();

        getLog().beginWrite();
        for (int i = 0; i < 1024; ++i) {
            getLog().write(emptyLoggable);
        }
        getLog().endWrite();
        closeLog();
        initLog(1);
    }

    @Test
    public void testWrite2() throws IOException {
        initLog(111); // file size must be multiple of 2 to avoid alignment
        getLog().beginWrite();
        for (int j = 0; j < 111 * 1024; ++j) {
            Assert.assertEquals(3 * j, (int) getLog().write(DUMMY_LOGGABLE));
        }
        getLog().flush();
        getLog().endWrite();
        Assert.assertEquals(3, (int) getLog().getNumberOfFiles()); // each DUMMY_LOGGABLE takes 2 bytes
    }

    @Test
    public void testRemoveFile() throws IOException {
        initLog(1);
        getLog().beginWrite();
        for (int j = 0; j < 1024 * 99; ++j) {
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
    public void testRemoveFileInvalidAddress() throws IOException {
        initLog(1);
        getLog().beginWrite();
        for (int j = 0; j < 1024 * 10; ++j) {
            getLog().write(NullLoggable.create());
        }
        getLog().flush();
        getLog().endWrite();
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                getLog().removeFile(1111);
            }
        }, ExodusException.class);
    }

    @Test
    public void testRemoveFileNonexistentAddress() throws IOException {
        initLog(1);
        getLog().beginWrite();
        for (int j = 0; j < 1024 * 10; ++j) {
            getLog().write(NullLoggable.create());
        }
        getLog().flush();
        getLog().endWrite();
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                getLog().removeFile(1024 * 10);
            }
        }, ExodusException.class);
    }

    @Test
    public void testPaddingWithNulls() throws IOException {
        initLog(1);
        getLog().beginWrite();
        for (int i = 0; i < 100; ++i) {
            getLog().write(DUMMY_LOGGABLE);
            getLog().doPadWithNulls();
            Assert.assertEquals(i + 1, getLog().ensureWriter().getFileSetMutable().size());
        }
        getLog().endWrite();
    }

    @Test
    public void testWriteNulls() throws IOException {
        initLog(1);
        log.beginWrite();
        for (int j = 0; j < 1024 * 99; ++j) {
            getLog().write(NullLoggable.create());
        }
        log.endWrite();
        Assert.assertEquals(99, (int) getLog().getNumberOfFiles()); // null loggable should take only one byte
    }

    @Test
    public void testAutoAlignment() throws IOException {
        initLog(1);
        log.beginWrite();
        for (int i = 0; i < 1023; ++i) {
            getLog().write(NullLoggable.create());
        }
        // here auto-alignment should happen
        final long dummyAddress = getLog().write(DUMMY_LOGGABLE);
        getLog().endWrite();
        Assert.assertEquals(2, (int) getLog().getNumberOfFiles());
        // despite the fact that DUMMY_LOGGABLE size + 1023 nulls should result in 1025 bytes,
        // we should actually get 1026 (one kb + 2) due to automatic alignment
        Assert.assertEquals(1027, (int) getLog().getHighAddress());
        // by the same reason, address of dummy should be at the beginning of the second file
        Assert.assertEquals(1024, (int) dummyAddress);
    }

    @Test
    public void testWriteReadSameAddress() throws IOException {
        initLog(1);
        for (int i = 0; i < 100; ++i) {
            getLog().beginWrite();
            final long dummyAddress = getLog().write(DUMMY_LOGGABLE);
            getLog().endWrite();
            Assert.assertEquals((long) (i * 3), dummyAddress);
            Assert.assertEquals(dummyAddress, getLog().read(dummyAddress).getAddress());
        }
    }

    @Test
    public void testAutoAlignment2() throws IOException {
        initLog(1);
        // one kb loggable can't be placed in a single file of one kb size
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                try {
                    log.beginWrite();
                    getLog().write(ONE_KB_LOGGABLE);
                } catch (Throwable t) {
                    getLog().abortWrite();
                    throw t;
                }
            }
        }, TooBigLoggableException.class);
    }

    @Test
    public void testReadUnknownLoggableType() throws IOException {
        getLog().beginWrite();
        getLog().write(DUMMY_LOGGABLE);
        getLog().endWrite();
        getLog().read(0);
    }

    @Test
    public void testSetHighAddress() {
        final int loggablesCount = 350000; // enough number to make sure two files will be created
        getLog().beginWrite();
        for (int i = 0; i < loggablesCount; ++i) {
            getLog().write(DUMMY_LOGGABLE);
        }
        getLog().flush();
        getLog().setHighAddress(getLog().endWrite(), 3);
        final Iterator<RandomAccessLoggable> loggablesIterator = getLog().getLoggableIterator(0);
        loggablesIterator.next();
        Assert.assertFalse(loggablesIterator.hasNext());
    }

    @Test
    public void testSetHighAddress2() {
        final int loggablesCount = 350000; // enough number to make sure two files will be created
        getLog().beginWrite();
        for (int i = 0; i < loggablesCount; ++i) {
            getLog().write(DUMMY_LOGGABLE);
        }
        getLog().setHighAddress(getLog().endWrite(), 0);
        Assert.assertFalse(getLog().getLoggableIterator(0).hasNext());
    }

    @Test
    @TestFor(issues = "XD-317")
    public void testSetHighAddress_XD_317() {
        getLog().beginWrite();
        getLog().write(DUMMY_LOGGABLE);
        getLog().setHighAddress(getLog().endWrite(), 0);
        Assert.assertFalse(getLog().getLoggableIterator(0).hasNext());
        getLog().beginWrite();
        getLog().write(NullLoggable.create());
        getLog().endWrite();
        final Iterator<RandomAccessLoggable> it = getLog().getLoggableIterator(0);
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(NullLoggable.isNullLoggable(it.next()));
        Assert.assertFalse(it.hasNext());
        Assert.assertNull(it.next());
    }

    @Test
    @TestFor(issues = "XD-484")
    public void testSetHighAddress_XD_484() throws IOException {
        testSetHighAddress2();
        closeLog();
        Assert.assertEquals(0L, getLog().getHighAddress());
    }

    @Test
    public void testWriteImmediateRead() throws IOException {
        testWriteImmediateRead(1, 1024);
    }

    @Test
    public void testWriteImmediateRead2() throws IOException {
        testWriteImmediateRead(4, 1024 * 4);
    }

    @Test
    public void testWriteImmediateRead3() throws IOException {
        testWriteImmediateRead(16, 1024 * 16);
    }

    @Test
    public void testWriteImmediateRead4() throws IOException {
        testWriteImmediateRead(2, 1024);
    }

    @Test
    public void testWriteSequentialRead() throws IOException {
        testWriteSequentialRead(1, 1024);
    }

    @Test
    public void testWriteSequentialRead2() throws IOException {
        testWriteSequentialRead(4, 1024 * 4);
    }

    @Test
    public void testWriteSequentialRead3() throws IOException {
        testWriteSequentialRead(16, 1024 * 16);
    }

    @Test
    public void testWriteSequentialRead4() throws IOException {
        testWriteSequentialRead(2, 1024);
    }

    @Test
    public void testWriteRandomRead() throws IOException {
        testWriteRandomRead(1, 1024);
    }

    @Test
    public void testWriteRandomRead2() throws IOException {
        testWriteRandomRead(4, 1024 * 4);
    }

    @Test
    public void testWriteRandomRead3() throws IOException {
        testWriteRandomRead(16, 1024 * 16);
    }

    @Test
    public void testWriteRandomRead4() throws IOException {
        testWriteRandomRead(2, 1024);
    }

    @Test
    public void testAllLoggablesIterator() throws IOException {
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
            Assert.assertEquals((long) (4 * i++), l.getAddress());
            Assert.assertEquals(127, l.getType());
            Assert.assertEquals(1, l.getDataLength());
        }
        Assert.assertEquals(count, i);
    }

    private long writeData(ByteIterable iterable) {
        return getLog().write((byte) 127, Loggable.NO_STRUCTURE_ID, iterable);
    }

    @Test
    public void testAllRandomAccessLoggablesIterator() throws IOException {
        initLog(4, 1024 * 4);
        final int count = 10;
        getLog().beginWrite();
        for (int i = 0; i < count; ++i) {
            writeData(CompressedUnsignedLongByteIterable.getIterable(i));
        }
        getLog().endWrite();
        final Iterator<RandomAccessLoggable> it = getLog().getLoggableIterator(0);
        int i = 0;
        while (it.hasNext()) {
            Loggable l = it.next();
            Assert.assertEquals((long) (4 * i++), l.getAddress());
            Assert.assertEquals(127, l.getType());
            Assert.assertEquals(1, l.getDataLength());
        }
        Assert.assertEquals(count, i);
    }

    @Test
    public void testClearInvalidLog() throws IOException {
        initLog(2);
        getLog().beginWrite();
        for (int i = 0; i < 2048; ++i) {
            getLog().write(DUMMY_LOGGABLE);
        }
        getLog().endWrite();
        closeLog();
        try {
            initLog(1);
        } catch (ExodusException e) {
            return;
        }
        Assert.assertTrue("Expected exception wasn't thrown", true);
    }

    @Test
    public void testClearInvalidLog2() throws IOException {
        initLog(2);
        getLog().beginWrite();
        for (int i = 0; i < 2048; ++i) {
            getLog().write(DUMMY_LOGGABLE);
        }
        getLog().flush();
        getLog().endWrite();
        closeLog();
        initLog(new LogConfig().setFileSize(1).setClearInvalidLog(true));
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
            addrs.put(writeData(CompressedUnsignedLongByteIterable.getIterable(i)), valueOf(i));
        }
        log.endWrite();
        for (Long addr : addrs.keySet()) {
            Assert.assertEquals((int) addrs.get(addr), (int) CompressedUnsignedLongByteIterable.getLong(getLog().read(addr).getData()));
            getLog().read(addr);
        }
    }

    private static TestLoggable createNoDataLoggable(byte type) {
        return new TestLoggable(type, ByteIterable.EMPTY, Loggable.NO_STRUCTURE_ID);
    }

}
