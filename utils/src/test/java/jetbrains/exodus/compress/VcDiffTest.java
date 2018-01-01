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

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.util.ByteArraySizedInputStream;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.LightByteArrayOutputStream;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.URL;

public class VcDiffTest extends BaseCompressTest {

    private static final String SAMPLE_TEXT =
            "The problem of distinguishing prime numbers from composite numbers and of resolving the latter into " +
                    "their prime factors is known to be one of the most important and useful in arithmetic. It has " +
                    "engaged the industry and wisdom of ancient and modern geometers to such an extent that it would " +
                    "be superfluous to discuss the problem at length. Further, the dignity of the science itself " +
                    "seems to require solution of a problem so elegant and so celebrated. " +
                    "C. F. Gauss, Disquisitiones Arithmeticae, Braunschweig, 1801. English Edition, Springer-Verlag, New York, 1986.";

    private static final char[] CHARS;

    static {
        CHARS = new char[32];
        int i = 0;
        for (char c = 'a'; c <= 'z'; ++c) {
            CHARS[i++] = c;
        }
        for (char c = '0'; i < CHARS.length; ++c) {
            CHARS[i++] = c;
        }
    }

    @Test
    public void testEmpty() throws IOException {
        testDiff("", "", false);
    }

    @Test
    public void testTrivial() throws IOException {
        testDiff(SAMPLE_TEXT, SAMPLE_TEXT.substring(0, 100) + '.' + SAMPLE_TEXT.substring(100), true);
        testDiff(SAMPLE_TEXT, SAMPLE_TEXT.substring(0, 100) + SAMPLE_TEXT.substring(101), true);
    }

    @Test
    public void testTrivial2() throws IOException {
        testDiff(SAMPLE_TEXT, SAMPLE_TEXT.substring(100) + '.' + SAMPLE_TEXT.substring(0, 100), true);
        testDiff(SAMPLE_TEXT, SAMPLE_TEXT.substring(100) + SAMPLE_TEXT.substring(0, 100), true);
        testDiff(SAMPLE_TEXT, SAMPLE_TEXT.substring(100), true);
    }

    @Test
    public void testUncompressed() throws IOException {
        final InputStream previousSource = toStream(SAMPLE_TEXT);
        previousSource.mark(Integer.MAX_VALUE);
        final String uncompressedText = "Sample text which couldn't be compressed against Gauss's quotation";
        final InputStream source = toStream(uncompressedText);
        final ByteArrayOutputStream delta = new ByteArrayOutputStream();
        final VcDiff diff = new VcDiff();
        Assert.assertFalse(diff.encode(source, previousSource, delta));
        Assert.assertTrue(delta.size() > uncompressedText.length());
        final LightByteArrayOutputStream decodedSource = new LightByteArrayOutputStream();
        previousSource.reset();
        diff.decode(previousSource, new ByteArrayInputStream(delta.toByteArray()), decodedSource);
        Assert.assertEquals(uncompressedText, toString(decodedSource));
        previousSource.reset();
        Assert.assertEquals(uncompressedText,
                toString(diff.decode(previousSource, new ByteArrayInputStream(delta.toByteArray()))));
    }

    @Test
    public void testWithoutPreviousContent() throws IOException {
        testDiff(null, SAMPLE_TEXT + SAMPLE_TEXT, true);
    }

    @Test
    public void testWithoutPreviousContentUncompressed() throws IOException {
        final Random rnd = new Random();
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            builder.append('0' + rnd.nextInt(16));
        }
        testDiff(null, builder.toString(), false);
    }

    @Test
    public void testLargeContent() {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 2000; ++i) {
            builder.append(SAMPLE_TEXT);
            builder.append("\n");
        }
        final String content = builder.toString();
        TestUtil.time("Testing large content", new Runnable() {
            @Override
            public void run() {
                try {
                    testDiff(content, content.substring(0, 100000) + " Exodus rules " + content.substring(100000), true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Test
    public void testLargeContentUncompressed() {
        final Random rnd = new Random();
        final StringBuilder[] builder = {new StringBuilder()};
        for (int i = 0; i < 32768 * 5; i++) {
            builder[0].append(CHARS[rnd.nextInt(32)]);
        }
        final String oldContent = builder[0].toString();
        builder[0] = new StringBuilder();
        for (int i = 0; i < 32768 * 10; i++) {
            builder[0].append(CHARS[rnd.nextInt(32)]);
        }
        TestUtil.time("Testing large uncompressed content", new Runnable() {
            @Override
            public void run() {
                try {
                    testDiff(oldContent, builder[0].toString(), false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Test()
    public void testLargeBinaryContent() {
        TestUtil.time("Testing large uncompressed content", new Runnable() {
            @Override
            public void run() {
                try {
                    testFilesDiff("jna0.jar", "jna1.jar", false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static void testDiff(@Nullable final String previousContent, @NotNull final String content, final boolean checkRatio) throws IOException {
        testDiff(toStream(previousContent), toStream(content), checkRatio);
    }

    private static void testFilesDiff(@NotNull final String file1, @NotNull final String file2, final boolean checkRatio) throws IOException {
        final ClassLoader classLoader = VcDiffTest.class.getClassLoader();
        final URL resource1 = classLoader.getResource(file1);
        try (InputStream stream1 = resource1.openStream()) {
            final URL resource2 = classLoader.getResource(file2);
            try (InputStream stream2 = resource2.openStream()) {
                final byte[] source1 = new byte[(int) new File(resource1.getFile()).length()];
                Assert.assertEquals(source1.length, IOUtil.readFully(stream1, source1));
                final byte[] source2 = new byte[(int) new File(resource2.getFile()).length()];
                Assert.assertEquals(source2.length, IOUtil.readFully(stream2, source2));
                testDiff(new ByteArraySizedInputStream(source1), new ByteArraySizedInputStream(source2), checkRatio);
            }
        }
    }

    private static void testDiff(@Nullable final ByteArraySizedInputStream previousSource, @NotNull final ByteArraySizedInputStream source, final boolean checkRatio) throws IOException {
        if (previousSource != null) {
            previousSource.mark(Integer.MAX_VALUE);
        }
        source.mark(Integer.MAX_VALUE);
        final VcDiff diff = new VcDiff();
        diff.getConfig().setTargetPercentage(85);
        ByteArrayOutputStream delta = new ByteArrayOutputStream();
        boolean result = diff.encode(source, previousSource, delta);
        if (checkRatio) {
            Assert.assertTrue(result);
            Assert.assertTrue(delta.size() < source.size() / 2);
        }
        if (previousSource != null) {
            previousSource.reset();
        }
        source.reset();
        Assert.assertTrue(TestUtil.streamsEqual(source, diff.decode(previousSource, new ByteArrayInputStream(delta.toByteArray()))));
        if (previousSource != null) {
            previousSource.reset();
        }
        source.reset();
        delta = new ByteArrayOutputStream();
        final OutputStream diffOutput = diff.encode(previousSource, delta);
        diffOutput.write(source.toByteArray());
        diffOutput.close();
        if (checkRatio) {
            Assert.assertTrue(delta.size() < source.size() / 2);
        }
        final LightByteArrayOutputStream decodedSource = new LightByteArrayOutputStream();
        if (previousSource != null) {
            previousSource.reset();
        }
        source.reset();
        diff.decode(previousSource, new ByteArrayInputStream(delta.toByteArray()), decodedSource);
        Assert.assertTrue(TestUtil.streamsEqual(source, new ByteArrayInputStream(decodedSource.toByteArray(), 0, decodedSource.size())));
    }
}
