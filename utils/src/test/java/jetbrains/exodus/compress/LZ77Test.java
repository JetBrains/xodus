/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.util.LightByteArrayOutputStream;
import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LZ77Test extends BaseCompressTest {

    private static final String SAMPLE_TEXT =
            "The problem of distinguishing prime numbers from composite numbers and of resolving the latter into " +
                    "their prime factors is known to be one of the most important and useful in arithmetic. It has " +
                    "engaged the industry and wisdom of ancient and modern geometers to such an extent that it would " +
                    "be superfluous to discuss the problem at length. Further, the dignity of the science itself " +
                    "seems to require solution of a problem so elegant and so celebrated. " +
                    "C. F. Gauss, Disquisitiones Arithmeticae, Braunschweig, 1801. English Edition, Springer-Verlag, New York, 1986.";

    @Test
    public void test0() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("12345678"));
        final List<LZ77.Match> output = encoder.encode(toStream("12345678"));
        Assert.assertEquals(1, output.size());
        final LZ77.Match match = output.get(0);
        Assert.assertEquals(8, match.offset);
        Assert.assertEquals(8, match.length);
    }

    @Test
    public void test1() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("1234567890"));
        final List<LZ77.Match> output = encoder.encode(toStream("12345678"));
        Assert.assertEquals(1, output.size());
        final LZ77.Match match = output.get(0);
        Assert.assertEquals(10, match.offset);
        Assert.assertEquals(8, match.length);
    }

    @Test
    public void test2() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("123456789999"));
        List<LZ77.Match> output = encoder.encode(toStream("12345678"));
        Assert.assertEquals(1, output.size());
        LZ77.Match match = output.get(0);
        Assert.assertEquals(12, match.offset);
        Assert.assertEquals(8, match.length);
        output = encoder.encode(toStream("9999"));
        Assert.assertEquals(1, output.size());
        match = output.get(0);
        Assert.assertEquals(12, match.offset);
        Assert.assertEquals(4, match.length);
    }

    @Test
    public void test3() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("1234567890"));
        final List<LZ77.Match> output = encoder.encode(toStream("1234567890"));
        Assert.assertEquals(3, output.size());
        LZ77.Match match = output.get(0);
        Assert.assertEquals(10, match.offset);
        Assert.assertEquals(8, match.length);
    }

    @Test
    public void test4() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.getConfig().setMinMatchLength(2);
        encoder.encode(toStream("1234567890"));
        final List<LZ77.Match> output = encoder.encode(toStream("1234567890"));
        Assert.assertEquals(2, output.size());
        LZ77.Match match = output.get(0);
        Assert.assertEquals(10, match.offset);
        Assert.assertEquals(8, match.length);
        match = output.get(1);
        Assert.assertEquals(10, match.offset);
        Assert.assertEquals(2, match.length);
    }

    @Test
    public void test5() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("12345678"));
        for (int i = 0; i < 100; ++i) {
            final List<LZ77.Match> output = encoder.encode(toStream("12345678"));
            Assert.assertEquals(1, output.size());
            final LZ77.Match match = output.get(0);
            Assert.assertEquals(8, match.length);
        }
    }

    @Test
    public void test6() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("12345678"), 4);
        encoder.encode(toStream("12345678"), 4);
        final List<LZ77.Match> output = encoder.encode(toStream("12341234"));
        Assert.assertEquals(1, output.size());
        final LZ77.Match match = output.get(0);
        Assert.assertEquals(8, match.offset);
        Assert.assertEquals(8, match.length);
    }

    @Test
    public void test7() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("12345678"), 4);
        final List<LZ77.Match> output = encoder.encode(toStream("12341234"));
        Assert.assertEquals(1, output.size());
        final LZ77.Match match = output.get(0);
        Assert.assertEquals(4, match.offset);
        Assert.assertEquals(8, match.length);
    }

    @Test
    public void test8() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        Pair<List<LZ77.Match>, Integer> result = encoder.encode(toStream("12"), 4);
        Assert.assertEquals(result.getSecond().intValue(), 2);
        result = encoder.encode(toStream("34"), 4);
        Assert.assertEquals(result.getSecond().intValue(), 2);
        result = encoder.encode(toStream("12"), 4);
        Assert.assertEquals(result.getSecond().intValue(), 2);
        result = encoder.encode(toStream("34"), 4);
        Assert.assertEquals(result.getSecond().intValue(), 2);
        final List<LZ77.Match> output = encoder.encode(toStream("12341234"));
        Assert.assertEquals(1, output.size());
        final LZ77.Match match = output.get(0);
        Assert.assertEquals(8, match.offset);
        Assert.assertEquals(8, match.length);
    }

    @Test
    public void test9() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.getConfig().setMinMatchLength(2);
        encoder.encode(toStream("1234567890"));
        final List<LZ77.Match> output = encoder.encode(toStream("1234567890"));
        Assert.assertEquals(2, output.size());
        LZ77.Match match = output.get(0);
        Assert.assertEquals(10, match.offset);
        Assert.assertEquals(8, match.length);
        match = output.get(1);
        Assert.assertEquals(10, match.offset);
        Assert.assertEquals(2, match.length);
        final LZ77 decoder = getSmallWindowEncoder();
        Assert.assertEquals(8, decoder.fillForDecode(toStream("1234567890"), 8));
        Assert.assertEquals(2, decoder.fillForDecode(toStream("90"), 8));
        final LightByteArrayOutputStream stream = new LightByteArrayOutputStream();
        decoder.decode(output, stream);
        Assert.assertEquals("1234567890", toString(stream));
    }

    @Test
    public void testMatchCount() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("12345678qwertyui"));
        final List<LZ77.Match> output = encoder.encode(toStream("12345678qwertyui"));
        Assert.assertEquals(1, output.size());
        final LZ77.Match match = output.get(0);
        Assert.assertEquals(16, match.offset);
        Assert.assertEquals(8, match.length);
        Assert.assertEquals(2, match.count);
    }

    @Test
    public void testMatchCount2() throws IOException {
        final LZ77 encoder = getSmallWindowEncoder();
        encoder.encode(toStream("aaabbbcccddd"));
        final List<LZ77.Match> output = encoder.encode(toStream("aaacccbbbddd"));
        Assert.assertEquals(4, output.size());
        final LZ77.Match match = output.get(1);
        Assert.assertEquals('c', match.offset);
        Assert.assertEquals(0, match.length);
        Assert.assertEquals(3, match.count);
    }

    @Test
    public void testEncodeDecodeSampleText() throws IOException {
        final LZ77 lz77 = getSmallWindowEncoder();
        final List<LZ77.Match> output = lz77.encode(toStream(SAMPLE_TEXT));
        lz77.reset();
        final LightByteArrayOutputStream stream = new LightByteArrayOutputStream();
        lz77.decode(output, stream);
        Assert.assertEquals(SAMPLE_TEXT, toString(stream));
    }

    @Test
    public void testBinaryContent() throws IOException {
        final LightByteArrayOutputStream content = new LightByteArrayOutputStream();
        for (int i = 0; i < 256; ++i) {
            content.write(i >> 3);
        }
        for (int i = 0; i < 256; ++i) {
            content.write(255 - (i >> 3));
        }
        final byte[] contentArray = content.toByteArray();
        // the following assert does not express the contract of the test, but is necessary for further testing
        Assert.assertEquals(contentArray.length, content.size());
        final LZ77 lz77 = getSmallWindowEncoder();
        final List<LZ77.Match> output = lz77.encode(new ByteArrayInputStream(contentArray, 0, content.size()));
        lz77.reset();
        final LightByteArrayOutputStream stream = new LightByteArrayOutputStream();
        lz77.decode(output, stream);
        Assert.assertTrue(Arrays.equals(contentArray, stream.toByteArray()));
    }

    @Test
    public void testEncodingDecodingBenchmark() throws IOException {
        final LZ77 lz77 = new LZ77(); // create encoder with default parameters
        final InputStream stream = toStream(SAMPLE_TEXT);
        stream.mark(Integer.MAX_VALUE);
        final List<LZ77.Match> output = new ArrayList<>();
        final long encodingStart = System.currentTimeMillis();
        for (int i = 0; i < 10000; ++i) {
            output.addAll(lz77.encode(stream));
            stream.reset();
        }
        final long decodingStart = System.currentTimeMillis();
        lz77.reset();
        final LightByteArrayOutputStream outputStream = new LightByteArrayOutputStream();
        lz77.decode(output, outputStream);
        final long decodingFinish = System.currentTimeMillis();
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10000; ++i) {
            builder.append(SAMPLE_TEXT);
        }
        Assert.assertEquals(builder.toString(), toString(outputStream));
        System.out.println("Encoding took: " + (decodingStart - encodingStart));
        System.out.println("Decoding took: " + (decodingFinish - decodingStart));
    }

    @Test
    public void testEncodingDecodingBenchmark2() throws IOException {
        final LZ77 lz77 = new LZ77(); // create encoder with default parameters
        final byte[] randomBytes = new byte[100000];
        final Random rnd = new Random();
        for (int i = 0; i < randomBytes.length; i++) {
            randomBytes[i] = (byte) rnd.next(8);
        }
        final InputStream stream = new ByteArrayInputStream(randomBytes);
        stream.mark(Integer.MAX_VALUE);
        final List<LZ77.Match> output = new ArrayList<>();
        final long encodingStart = System.currentTimeMillis();
        for (int i = 0; i < 10; ++i) {
            output.addAll(lz77.encode(stream));
            stream.reset();
        }
        final long decodingStart = System.currentTimeMillis();
        lz77.reset();
        final LightByteArrayOutputStream outputStream = new LightByteArrayOutputStream();
        lz77.decode(output, outputStream);
        final long decodingFinish = System.currentTimeMillis();
        final InputStream decodedStream = new ByteArrayInputStream(outputStream.toByteArray(), 0, outputStream.size());
        for (int i = 0; i < 10; ++i) {
            int c;
            while ((c = stream.read()) != -1) {
                Assert.assertEquals(c, decodedStream.read());
            }
            stream.reset();
        }
        System.out.println("Encoding took: " + (decodingStart - encodingStart));
        System.out.println("Decoding took: " + (decodingFinish - decodingStart));
    }

    private static LZ77 getSmallWindowEncoder() {
        final LZ77.Config config = new LZ77.Config();
        config.setMaxMatchLength(8);
        config.setWindowSize(128);
        return new LZ77(config);
    }
}
