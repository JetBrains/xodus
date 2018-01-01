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
package jetbrains.exodus.compress;

import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class VLQTest {

    @Test
    public void testEncodeDecode() throws IOException {
        final long[] randomLongs = new long[100000];
        final Random rnd = new Random();
        for (int i = 0; i < randomLongs.length; i++) {
            randomLongs[i] = Math.abs(rnd.nextLong());
        }
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (long randomLong : randomLongs) {
            VLQUtil.writeLong(randomLong, output);
        }
        final InputStream input = new ByteArrayInputStream(output.toByteArray());
        for (long randomLong : randomLongs) {
            Assert.assertEquals(randomLong, VLQUtil.readLong(input));
        }
    }

    @Test
    public void testEncodeDecodeCount() throws IOException {
        final int[] randomInts = new int[100000];
        final Random rnd = new Random();
        for (int i = 0; i < randomInts.length; i++) {
            randomInts[i] = rnd.nextInt(32768);
        }
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (int randomInt : randomInts) {
            VLQUtil.writeCount(randomInt, 32768, output);
        }
        final InputStream input = new ByteArrayInputStream(output.toByteArray());
        for (long randomLong : randomInts) {
            Assert.assertEquals(randomLong, VLQUtil.readCount(32768, input));
        }
    }
}
