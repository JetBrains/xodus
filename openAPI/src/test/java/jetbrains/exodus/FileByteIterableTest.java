/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileByteIterableTest {

    private static final String SAMPLE_CONTENT = "�? хорошо бы еще бы убрать всякую сладкую и калорийную гадость, и заменть ее более здоровыми закусками к чаю. Теми же фруктами, орешками";

    File file;

    @Before
    public void setUp() throws Exception {
        file = File.createTempFile("FileByteIterable", null, TestUtil.createTempDir());
    }

    @After
    public void tearDown() throws Exception {
        if (file.delete()) {
            final File dir = file.getParentFile();
            if (!dir.delete()) {
                dir.deleteOnExit();
            }
        }
    }

    @Test
    public void testEmptyIterable() throws IOException {
        final FileByteIterable it = new FileByteIterable(file);
        Assert.assertEquals(0, compare(it.iterator(), ByteIterable.EMPTY_ITERATOR));
    }

    @Test
    public void testSingleIterable() throws IOException {
        final FileOutputStream output = new FileOutputStream(file);
        try {
            output.write(SAMPLE_CONTENT.getBytes("UTF-8"));
        } finally {
            output.close();
        }
        final FileByteIterable it = new FileByteIterable(file);
        Assert.assertEquals(0, compare(it.iterator(), new ArrayByteIterable(SAMPLE_CONTENT.getBytes("UTF-8")).iterator()));
    }

    @Test
    public void testMultipleIterables() throws IOException {
        final int count = 10;
        final FileOutputStream output = new FileOutputStream(file);
        try {
            for (int i = 0; i < count; ++i) {
                output.write(SAMPLE_CONTENT.getBytes("UTF-8"));
            }
        } finally {
            output.close();
        }
        final byte[] sampleBytes = SAMPLE_CONTENT.getBytes("UTF-8");
        final int length = sampleBytes.length;
        for (int i = 0, offset = 0; i < count; ++i, offset += length) {
            Assert.assertEquals(0, compare(new FileByteIterable(file, offset, length).iterator(), new ArrayByteIterable(sampleBytes).iterator()));
        }
    }

    private static int compare(ByteIterator i1, ByteIterator i2) {
        while (true) {
            boolean hasNext1 = i1.hasNext();
            boolean hasNext2 = i2.hasNext();
            if (!hasNext1) {
                return hasNext2 ? -1 : 0;
            }
            if (!hasNext2) {
                return 1;
            }
            int cmp = (i1.next() & 0xff) - (i2.next() & 0xff);
            if (cmp != 0) {
                return cmp;
            }
        }
    }
}
