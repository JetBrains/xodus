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

import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.LightByteArrayOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Implementation of the VCDIFF-like delta encoding algorithm (http://en.wikipedia.org/wiki/VCDIFF).
 * Doesn't strictly follow RFC 3284 (http://tools.ietf.org/html/rfc3284).
 */
@SuppressWarnings("StatementWithEmptyBody")
public class VcDiff {

    @NotNull
    private final Config config;
    @NotNull
    private final Deflater compressor;
    @NotNull
    private final Inflater decompressor;
    @NotNull
    private final byte[] dictionaryBytes;
    @NotNull
    private final byte[] sourceBytes;
    @NotNull
    private final byte[] compressedBytes;

    public VcDiff() {
        this(Config.DEFAULT);
    }

    public VcDiff(@NotNull final Config config) {
        this.config = config;
        compressor = new Deflater(Deflater.BEST_COMPRESSION);
        decompressor = new Inflater();
        final int windowSize = config.getWindowSize();
        dictionaryBytes = new byte[windowSize / 2];
        sourceBytes = new byte[windowSize];
        // seems it's true that size of compressed output never can exceed size of input more than two times
        compressedBytes = new byte[windowSize * 2];
    }

    @NotNull
    public Config getConfig() {
        return config;
    }

    public void close() {
        compressor.end();
        decompressor.end();
    }

    public boolean encode(@NotNull final InputStream source,
                          @Nullable final InputStream previousSource,
                          @NotNull final OutputStream delta) throws IOException {
        final int windowSize = config.getWindowSize();
        final int halfWindowSize = windowSize / 2;

        // at first, copy first window of source into byte array
        final byte[] sourceWindow = new byte[halfWindowSize];
        final int actuallyRead = IOUtil.readFully(source, sourceWindow);
        if (actuallyRead < 1) {
            return true;
        }
        final LightByteArrayOutputStream tempOutput = new LightByteArrayOutputStream();
        encodeDiffWindow(
                new ByteArrayInputStream(sourceWindow, 0, actuallyRead), previousSource, tempOutput);
        final int tempOutputSize = tempOutput.size();
        if (tempOutputSize * 100 / actuallyRead > config.getTargetPercentage()) {
            // just copy the input
            VLQUtil.writeInt(0, delta); // window size = 0 means there is no compression
            delta.write(sourceWindow, 0, actuallyRead);
            IOUtil.copyStreams(source, delta, IOUtil.BUFFER_ALLOCATOR);
            return false;
        }
        // compression ratio is ok
        VLQUtil.writeInt(windowSize, delta); // window size != 0 means there is compression
        // save temp output
        delta.write(tempOutput.toByteArray(), 0, tempOutput.size());
        while (encodeDiffWindow(source, previousSource, delta) > 0) ;
        return true;
    }

    public OutputStream encode(@Nullable final InputStream previousSource,
                               @NotNull final OutputStream delta) {
        return encode(previousSource, delta, config);
    }

    public OutputStream encode(@Nullable final InputStream previousSource,
                               @NotNull final OutputStream delta,
                               @NotNull final Config config) {
        return new OutputStream() {

            private final int windowSize = config.getWindowSize();
            private LightByteArrayOutputStream tempOutput = new LightByteArrayOutputStream();
            private boolean ratioTested = false;

            @Override
            public void write(int b) throws IOException {
                if (tempOutput == null) {
                    delta.write(b);
                } else {
                    tempOutput.write(b);
                    flushTempOutputIfNecessary();
                }
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                if (tempOutput == null) {
                    delta.write(b, off, len);
                } else {
                    tempOutput.write(b, off, len);
                    flushTempOutputIfNecessary();
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    if (tempOutput != null) {
                        while (tempOutput.size() > 0) {
                            flushTempOutput();
                            // tempOutput == null means there is no compression.
                            // This can happen on the first attempt to flush temp output. In that case we
                            // should just close the delta output without writing end of file marker
                            if (tempOutput == null) {
                                return;
                            }
                        }
                        if (ratioTested) {
                            // write end of file
                            VLQUtil.writeInt(0, delta);
                        }
                    }
                } finally {
                    delta.close();
                }
            }

            private void flushTempOutputIfNecessary() throws IOException {
                while (tempOutput != null && tempOutput.size() >= windowSize) {
                    flushTempOutput();
                }
            }

            private void flushTempOutput() throws IOException {
                final int size = tempOutput.size();
                if (size > 0) {
                    final byte[] tempOutputBytes = tempOutput.toByteArray();
                    final int actuallyRead;
                    if (ratioTested) {
                        actuallyRead = encodeDiffWindow(new ByteArrayInputStream(
                                tempOutputBytes, 0, size), previousSource, delta);
                    } else {
                        final LightByteArrayOutputStream testOutput = new LightByteArrayOutputStream();
                        actuallyRead = encodeDiffWindow(new ByteArrayInputStream(
                                tempOutputBytes, 0, size), previousSource, testOutput);
                        final int testOutputSize = testOutput.size();
                        if (testOutputSize * 100 / actuallyRead > config.getTargetPercentage()) {
                            // we are going to write uncompressed data, run corresponding closure if any
                            final Runnable beforeWritingUncompressedClosure = config.getBeforeWritingUncompressedClosure();
                            if (beforeWritingUncompressedClosure != null) {
                                beforeWritingUncompressedClosure.run();
                            }
                            VLQUtil.writeInt(0, delta); // window size = 0 means there is no compression
                            delta.write(tempOutputBytes, 0, size);
                            tempOutput = null;
                            return;
                        }
                        // compression ratio is ok
                        ratioTested = true;
                        // we are going to write compressed data, run corresponding closure if any
                        final Runnable beforeWritingCompressedClosure = config.getBeforeWritingCompressedClosure();
                        if (beforeWritingCompressedClosure != null) {
                            beforeWritingCompressedClosure.run();
                        }
                        VLQUtil.writeInt(windowSize, delta); // window size != 0 means there is compression
                        delta.write(testOutput.toByteArray(), 0, testOutputSize);
                    }
                    if (actuallyRead == size) {
                        tempOutput = new LightByteArrayOutputStream();
                    } else {
                        final int newSize = size - actuallyRead;
                        System.arraycopy(tempOutputBytes, actuallyRead, tempOutputBytes, 0, newSize);
                        tempOutput.setSize(newSize);
                    }
                }
            }
        };
    }

    public void decode(@Nullable final InputStream previousSource,
                       @NotNull final InputStream delta,
                       @NotNull final OutputStream source) throws IOException {

        final int windowSize;
        try {
            windowSize = VLQUtil.readInt(delta);
        } catch (EOFException e) {
            return;
        }


        // if window size is 0 then source was just copied as is without compression
        if (windowSize == 0) {
            IOUtil.copyStreams(delta, source, IOUtil.BUFFER_ALLOCATOR);
            return;
        }

        while (!decodeDiffWindow(previousSource, delta, source)) ;
    }

    public InputStream decode(@Nullable final InputStream previousSource,
                              @NotNull final InputStream delta) throws IOException {
        return new InputStream() {

            private int windowSize = -1;
            private InputStream tempStream = null;
            private boolean eof = false;

            @Override
            public int read() throws IOException {
                if (readHeader()) {
                    while (true) {
                        if (tempStream != null) {
                            final int result = tempStream.read();
                            if (result >= 0) {
                                return result;
                            }
                        }
                        if (eof) {
                            break;
                        }
                        final LightByteArrayOutputStream tempOutput = new LightByteArrayOutputStream();
                        eof = decodeDiffWindow(previousSource, delta, tempOutput);
                        tempStream = eof ? null : new ByteArrayInputStream(tempOutput.toByteArray(), 0, tempOutput.size());
                    }
                }
                return windowSize == 0 ? delta.read() : -1;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (readHeader()) {
                    while (true) {
                        if (tempStream != null) {
                            final int result = tempStream.read(b, off, len);
                            if (result > 0) {
                                return result;
                            }
                        }
                        if (eof) {
                            break;
                        }
                        final LightByteArrayOutputStream tempOutput = new LightByteArrayOutputStream();
                        eof = decodeDiffWindow(previousSource, delta, tempOutput);
                        tempStream = eof ? null : new ByteArrayInputStream(tempOutput.toByteArray(), 0, tempOutput.size());
                    }
                }
                return windowSize == 0 ? delta.read(b, off, len) : -1;
            }

            // returns true if delta should be uncompressed
            private boolean readHeader() throws IOException {
                if (windowSize == -1) {
                    try {
                        windowSize = VLQUtil.readInt(delta);
                    } catch (EOFException e) {
                        return false;
                    }
                }
                return windowSize > 0;
            }
        };
    }

    // returns number of bytes actually read from source
    private int encodeDiffWindow(@NotNull final InputStream source,
                                 @Nullable final InputStream previousSource,
                                 @NotNull final OutputStream delta) throws IOException {
        final int dictionarySize = previousSource == null ? 0 : IOUtil.readFully(previousSource, dictionaryBytes);
        compressor.reset();
        final int windowSize = config.getWindowSize();
        final int actuallyRead = IOUtil.readFully(source, sourceBytes, windowSize - dictionarySize);
        if (actuallyRead <= 0) {
            VLQUtil.writeInt(0, delta);
        } else {
            if (dictionarySize > 0) {
                compressor.setDictionary(dictionaryBytes, 0, dictionarySize);
            }
            compressor.setInput(sourceBytes, 0, actuallyRead);
            compressor.finish();
            final int compressedSize = compressor.deflate(compressedBytes);
            VLQUtil.writeInt(compressedSize, delta);
            delta.write(compressedBytes, 0, compressedSize);
        }
        return actuallyRead;
    }

    // returns true if eof reached
    private boolean decodeDiffWindow(@Nullable final InputStream previousSource,
                                     @NotNull final InputStream delta,
                                     @NotNull final OutputStream output) throws IOException {
        final int compressedSize = VLQUtil.readInt(delta);
        if (compressedSize > 0) {

            final int dictionarySize;
            if (previousSource == null) {
                dictionarySize = 0;
            } else {
                dictionarySize = IOUtil.readFully(previousSource, dictionaryBytes);
            }

            decompressor.reset();

            try {
                if (IOUtil.readFully(delta, compressedBytes, compressedSize) != compressedSize) {
                    throw new DataFormatException();
                }
                decompressor.setInput(compressedBytes);
                int sourceSize = decompressor.inflate(sourceBytes);
                if (dictionarySize > 0) {
                    if (decompressor.needsDictionary()) {
                        decompressor.getAdler();
                        decompressor.setDictionary(dictionaryBytes, 0, dictionarySize);
                        sourceSize = decompressor.inflate(sourceBytes);
                    }
                }
                output.write(sourceBytes, 0, sourceSize);
            } catch (DataFormatException e) {
                throw new RuntimeException(e);
            }
        }
        return compressedSize == 0;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static final class Config {

        public static final Config DEFAULT = new Config();

        private static final int DEFAULT_WINDOW_SIZE = 32768;
        private static final int DEFAULT_TARGET_PERCENTAGE = 50;

        private int windowSize;
        private int targetPercentage;
        @Nullable
        private Runnable beforeWritingCompressedClosure;
        @Nullable
        private Runnable beforeWritingUncompressedClosure;

        public Config() {
            windowSize = DEFAULT_WINDOW_SIZE;
            targetPercentage = DEFAULT_TARGET_PERCENTAGE;
            beforeWritingCompressedClosure = null;
            beforeWritingUncompressedClosure = null;
        }

        private Config(@NotNull final Config source) {
            windowSize = source.windowSize;
            targetPercentage = source.targetPercentage;
            beforeWritingCompressedClosure = source.beforeWritingCompressedClosure;
            beforeWritingUncompressedClosure = source.beforeWritingUncompressedClosure;
        }

        public Config getClone() {
            return new Config(this);
        }

        public int getWindowSize() {
            return windowSize;
        }

        public void setWindowSize(final int windowSize) {
            this.windowSize = windowSize;
        }

        public int getTargetPercentage() {
            return targetPercentage;
        }

        public void setTargetPercentage(final int targetPercentage) {
            this.targetPercentage = targetPercentage;
        }

        @Nullable
        public Runnable getBeforeWritingCompressedClosure() {
            return beforeWritingCompressedClosure;
        }

        public void setBeforeWritingCompressedClosure(@Nullable final Runnable beforeWritingCompressedClosure) {
            this.beforeWritingCompressedClosure = beforeWritingCompressedClosure;
        }

        @Nullable
        public Runnable getBeforeWritingUncompressedClosure() {
            return beforeWritingUncompressedClosure;
        }

        public void setBeforeWritingUncompressedClosure(@Nullable final Runnable beforeWritingUncompressedClosure) {
            this.beforeWritingUncompressedClosure = beforeWritingUncompressedClosure;
        }
    }
}
