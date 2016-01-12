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
import jetbrains.exodus.core.dataStructures.hash.HashUtil;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * LZ77 universal coding (http://en.wikipedia.org/wiki/LZ77_and_LZ78) using binary search tree for
 * searching for longest matches.
 * Input sequence of bytes if encoded as sequence of Matches.
 */
public class LZ77 {

    @NotNull
    private final Config config;
    @NotNull
    private final byte[] window;
    private int readCursor;
    private int writeCursor;
    private final NavigableSet<Offset>[] searchTrees;

    public LZ77() {
        this(Config.DEFAULT);
    }

    public LZ77(@NotNull final Config config) {
        this.config = config;
        final int windowSize = config.getWindowSize();
        window = new byte[windowSize];
        //noinspection unchecked
        searchTrees = new NavigableSet[HashUtil.getCeilingPrime(windowSize / 23)];
        reset();
    }

    @NotNull
    public Config getConfig() {
        return config;
    }

    /**
     * Encodes count bytes of stream in to list of string matches.
     *
     * @param stream source stream
     * @param count  encode that count of bytes of source stream
     * @return list of matches and the number of actually read bytes
     * @throws IOException
     */
    public Pair<List<Match>, Integer> encode(@NotNull final InputStream stream, final int count) throws IOException {
        final LimitedInputStream limitedInputStream = new LimitedInputStream(stream, count);
        final List<Match> matchesList = encode(limitedInputStream);
        return new Pair<>(matchesList, limitedInputStream.bytesRead());
    }

    /**
     * Encodes stream into list of string matches.
     *
     * @param stream source stream
     * @return list of matches
     * @throws IOException
     */
    public List<Match> encode(@NotNull final InputStream stream) throws IOException {
        final int minMatchLength = config.getMinMatchLength();
        final int maxMatchLength = config.getMaxMatchLength();
        final int windowSize = window.length;
        final List<Match> result = new ArrayList<>();
        boolean eof = false;

        while (!eof) {
            int cursorsDiff = writeCursor - readCursor;
            if (cursorsDiff < 0) {
                cursorsDiff += windowSize;
            }
            while (cursorsDiff < maxMatchLength) {
                final int nextByte = stream.read();
                if (nextByte == -1) {
                    eof = true;
                    break;
                }
                getSearchTree(writeCursor).remove(new RemoveOffset(writeCursor));
                window[writeCursor] = (byte) nextByte;
                int addedOffset = writeCursor + windowSize - maxMatchLength;
                if (addedOffset >= windowSize) {
                    addedOffset -= windowSize;
                }
                getSearchTree(addedOffset).add(new Offset(addedOffset));
                if (++writeCursor == windowSize) {
                    writeCursor = 0;
                }
                ++cursorsDiff;
            }
            while (readCursor != writeCursor) {
                final SearchOffset matchOffset = new SearchOffset(readCursor);
                getSearchTree(readCursor).contains(matchOffset);
                int remainedBytes = writeCursor + windowSize - readCursor;
                if (remainedBytes >= windowSize) {
                    remainedBytes -= windowSize;
                }
                int bestMatchLength = matchOffset.getBestMatchLength();
                if (bestMatchLength > remainedBytes) {
                    bestMatchLength = remainedBytes;
                }
                final int resultSize = result.size();
                final Match lastMatch = resultSize > 0 ? result.get(resultSize - 1) : null;
                Match newMatch = null;
                if (bestMatchLength < minMatchLength) {
                    final byte b = window[readCursor];
                    if (lastMatch == null || lastMatch.length != 0 || lastMatch.offset != (((int) b) & 0xff)) {
                        newMatch = new Match(b);
                    }
                    ++readCursor;
                } else {
                    final int bestMatchOffset = (readCursor + windowSize - matchOffset.getBestMatchOffset()) % windowSize;
                    if (lastMatch == null || lastMatch.offset != bestMatchOffset || lastMatch.length != bestMatchLength) {
                        newMatch = new Match(bestMatchOffset, bestMatchLength);
                    }
                    readCursor += bestMatchLength;
                }
                if (newMatch != null) {
                    result.add(newMatch);
                } else {
                    lastMatch.repeat();
                }
                if (readCursor >= windowSize) {
                    readCursor -= windowSize;
                }
                if (!eof) {
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Does the same as {@link LZ77#encodeImmutable(java.io.InputStream)}, but for limited count of bytes of the
     * source stream.
     *
     * @param stream source stream
     * @param count  encode that count of bytes of source stream
     * @return list of matches and the number of actually read bytes
     * @throws IOException
     */
    public Pair<List<Match>, Integer> encodeImmutable(@NotNull final InputStream stream, final int count) throws IOException {
        final LimitedInputStream limitedInputStream = new LimitedInputStream(stream, count);
        final List<Match> matchesList = encodeImmutable(limitedInputStream);
        return new Pair<>(matchesList, limitedInputStream.bytesRead());
    }

    /**
     * Encodes stream into list of string matches not affecting search trees. This method can be used in conjunction
     * with {@link jetbrains.exodus.compress.LZ77#fillForEncode(java.io.InputStream, int)}, which does update search
     * trees, so that the source stream will be encoded against pre-filled content.
     *
     * @param stream source stream
     * @return list of matches
     * @throws IOException
     */
    public List<Match> encodeImmutable(@NotNull final InputStream stream) throws IOException {
        final int minMatchLength = config.getMinMatchLength();
        final int maxMatchLength = config.getMaxMatchLength();
        final int windowSize = window.length;
        final List<Match> result = new ArrayList<>();
        boolean eof = false;

        while (!eof) {
            int cursorsDiff = writeCursor - readCursor;
            if (cursorsDiff < 0) {
                cursorsDiff += windowSize;
            }
            while (cursorsDiff < maxMatchLength) {
                final int nextByte = stream.read();
                if (nextByte == -1) {
                    eof = true;
                    break;
                }
                window[writeCursor] = (byte) nextByte;
                if (++writeCursor == windowSize) {
                    writeCursor = 0;
                }
                ++cursorsDiff;
            }
            while (readCursor != writeCursor) {
                final SearchOffset matchOffset = new SearchOffset(readCursor);
                getSearchTree(readCursor).contains(matchOffset);
                int remainedBytes = writeCursor + windowSize - readCursor;
                if (remainedBytes >= windowSize) {
                    remainedBytes -= windowSize;
                }
                int bestMatchLength = matchOffset.getBestMatchLength();
                if (bestMatchLength > remainedBytes) {
                    bestMatchLength = remainedBytes;
                }
                final int resultSize = result.size();
                final Match lastMatch = resultSize > 0 ? result.get(resultSize - 1) : null;
                Match newMatch = null;
                if (bestMatchLength < minMatchLength) {
                    final byte b = window[readCursor];
                    if (lastMatch == null || lastMatch.length != 0 || lastMatch.offset != (((int) b) & 0xff)) {
                        newMatch = new Match(b);
                    }
                    ++readCursor;
                } else {
                    final int bestMatchOffset = (readCursor + windowSize - matchOffset.getBestMatchOffset()) % windowSize;
                    if (lastMatch == null || lastMatch.offset != bestMatchOffset || lastMatch.length != bestMatchLength) {
                        newMatch = new Match(bestMatchOffset, bestMatchLength);
                    }
                    readCursor += bestMatchLength;
                }
                if (newMatch != null) {
                    result.add(newMatch);
                } else {
                    lastMatch.repeat();
                }
                if (readCursor >= windowSize) {
                    readCursor -= windowSize;
                }
                if (!eof) {
                    break;
                }
            }
        }
        return result;
    }

    public void decode(@NotNull final List<Match> matches, @NotNull final OutputStream stream) throws IOException {
        final int windowSize = window.length;
        for (final Match match : matches) {
            final int matchLength = match.length;
            if (matchLength == 0) {
                final int nextByte = match.offset;
                for (int n = 0; n < match.count; ++n) {
                    stream.write(nextByte);
                    window[writeCursor] = (byte) nextByte;
                    if (++writeCursor == windowSize) {
                        writeCursor = 0;
                    }
                }
            } else {
                for (int n = 0; n < match.count; ++n) {
                    for (int i = 0, offset = (writeCursor + windowSize - match.offset) % windowSize; i < matchLength; ++i) {
                        final byte nextByte = window[offset];
                        stream.write(nextByte & 0xff);
                        window[writeCursor] = nextByte;
                        if (++writeCursor == windowSize) {
                            writeCursor = 0;
                        }
                        if (++offset == windowSize) {
                            offset = 0;
                        }
                    }
                }
            }
        }
    }

    /**
     * Pre-fills window for encoding against some known content.
     *
     * @param stream content stream
     * @param count  read that count of bytes
     * @return number of actually read bytes
     * @throws IOException
     */
    public int fillForEncode(@NotNull final InputStream stream, final int count) throws IOException {
        final int windowSize = window.length;
        int writeCursorSaved = writeCursor;
        int i = 0;
        for (; i < count; ++i) {
            final int nextByte = stream.read();
            if (nextByte == -1) {
                break;
            }
            window[writeCursor] = (byte) nextByte;
            if (++writeCursor == windowSize) {
                writeCursor = 0;
            }
        }
        readCursor = writeCursor;
        final int maxMatchLen = config.getMaxMatchLength();
        for (int j = 0; j < i - maxMatchLen; ++j) {
            getSearchTree(writeCursorSaved).add(new Offset(writeCursorSaved));
            if (++writeCursorSaved == windowSize) {
                writeCursorSaved = 0;
            }
        }
        return i;
    }

    /**
     * Pre-fills window for decoding against some known content.
     *
     * @param stream content stream
     * @param count  read that count of bytes
     * @return number of actually read bytes
     * @throws IOException
     */
    public int fillForDecode(@NotNull final InputStream stream, final int count) throws IOException {
        final int windowSize = window.length;
        int i = 0;
        for (; i < count; ++i) {
            final int nextByte = stream.read();
            if (nextByte == -1) {
                break;
            }
            window[writeCursor] = (byte) nextByte;
            if (++writeCursor == windowSize) {
                writeCursor = 0;
            }
        }
        return i;
    }

    public void reset() {
        readCursor = writeCursor = 0;
        int f1 = 0;
        int f2 = 1;
        for (int i = 0; i < window.length; i++) {
            int t = f2;
            f2 = (f2 + f1);
            if (f2 >= 63997) {
                f2 -= 63997;
            }
            f1 = t;
            window[i] = (byte) f2;
        }
        for (int i = 0; i < searchTrees.length; i++) {
            searchTrees[i] = new TreeSet<>();
        }
    }

    private NavigableSet<Offset> getSearchTree(final int offset) {
        final int windowSize = window.length;
        final int minMatchLen = config.getMinMatchLength();
        int n = 0;
        for (int i = 0, j = offset; i < minMatchLen; ++i) {
            n = (n * 251) + (window[j] & 0xff);
            if (++j == windowSize) {
                j = 0;
            }
        }
        return searchTrees[(n & 0x7fffffff) % searchTrees.length];
    }

    public static final class Config {

        public static final Config DEFAULT = new Config();

        // minimum value for minimum match length
        private static final int MIN_MIN_MATCH_LEN = 2;
        // maximum value for minimum match length
        private static final int MAX_MIN_MATCH_LEN = 16;
        // minimum value for maximum match length
        private static final int MIN_MAX_MATCH_LEN = 4;
        // maximum value for maximum match length
        private static final int MAX_MAX_MATCH_LEN = 1024;
        // minimum window size
        private static final int MIN_WINDOW_SIZE = 16;
        // maximum window size (16MB)
        private static final int MAX_WINDOW_SIZE = 1 << 24;

        private int minMatchLength;
        private int maxMatchLength;
        private int windowSize;

        public Config() {
            minMatchLength = 4;
            maxMatchLength = 259; // size of the range [minMatchLength..maxMatchLength] is power of 2
            windowSize = 65536;
        }

        public int getMinMatchLength() {
            return minMatchLength;
        }

        public void setMinMatchLength(final int minMatchLength) {
            if (minMatchLength > maxMatchLength) {
                throw new IllegalArgumentException("Minimum match length cannot be greater than maximum match length");
            }
            if (minMatchLength < MIN_MIN_MATCH_LEN) {
                throw new IllegalArgumentException("Minimum match length cannot be less than " + MIN_MIN_MATCH_LEN);
            }
            if (minMatchLength > MAX_MIN_MATCH_LEN) {
                throw new IllegalArgumentException("Minimum match length cannot be greater than " + MAX_MIN_MATCH_LEN);
            }
            this.minMatchLength = minMatchLength;
        }

        public int getMaxMatchLength() {
            return maxMatchLength;
        }

        public void setMaxMatchLength(final int maxMatchLength) {
            if (maxMatchLength < minMatchLength) {
                throw new IllegalArgumentException("Maximum match length cannot be less than minimum match length");
            }
            if (maxMatchLength > windowSize / 2) {
                throw new IllegalArgumentException("Maximum match length cannot be greater than half of window size");
            }
            if (maxMatchLength < MIN_MAX_MATCH_LEN) {
                throw new IllegalArgumentException("Maximum match length cannot be less than " + MIN_MAX_MATCH_LEN);
            }
            if (maxMatchLength > MAX_MAX_MATCH_LEN) {
                throw new IllegalArgumentException("Maximum match length cannot be greater than " + MAX_MAX_MATCH_LEN);
            }
            this.maxMatchLength = maxMatchLength;
        }

        public int getWindowSize() {
            return windowSize;
        }

        public void setWindowSize(final int windowSize) {
            if (windowSize < maxMatchLength || windowSize < MIN_WINDOW_SIZE || windowSize > MAX_WINDOW_SIZE) {
                throw new IllegalArgumentException();
            }
            this.windowSize = windowSize;
        }
    }

    /**
     * Represents a match. If length == 0 then offset contains a single byte.
     */
    public static final class Match {

        public final int offset;
        public final int length;
        public int count;

        public Match(byte b) {
            offset = ((int) b) & 0xff;
            length = 0;
            count = 1;
        }

        public Match(int offset, int length) {
            this.offset = offset;
            this.length = length;
            count = 1;
        }

        public void repeat() {
            ++count;
        }

        public void setCount(final int count) {
            this.count = count;
        }
    }

    private class Offset implements Comparable<Offset> {

        protected final int offset;

        private Offset(int offset) {
            this.offset = offset;
        }

        @Override
        public int compareTo(final Offset right) {
            if (right instanceof RemoveOffset) {
                return -right.compareTo(this);
            }
            final int maxMatchLength = config.getMaxMatchLength();
            int thisOffset = offset;
            int rightOffset = right.offset;
            if (thisOffset == rightOffset) {
                return 0;
            }
            final int windowSize = window.length;
            int result;
            int i = 0;
            if (thisOffset + maxMatchLength <= windowSize && rightOffset + maxMatchLength <= windowSize) {
                for (; ; ++thisOffset, ++rightOffset) {
                    result = window[thisOffset] - window[rightOffset];
                    if (result != 0) {
                        break;
                    }
                    if (++i == maxMatchLength) {
                        break;
                    }
                }
            } else {
                do {
                    result = window[thisOffset] - window[rightOffset];
                    if (result != 0) {
                        break;
                    }
                    if (++thisOffset == windowSize) {
                        thisOffset = 0;
                    }
                    if (++rightOffset == windowSize) {
                        rightOffset = 0;
                    }
                } while (++i < maxMatchLength);
            }
            updateBestMatch(right.offset, i);
            right.updateBestMatch(offset, i);
            return result;
        }

        protected void updateBestMatch(final int matchOffset, final int matchLength) {
        }
    }

    private class SearchOffset extends Offset {

        private int bestMatchOffset;
        private int bestMatchLength;

        private SearchOffset(int offset) {
            super(offset);
            bestMatchOffset = bestMatchLength = 0;
        }

        public int getBestMatchOffset() {
            return bestMatchOffset;
        }

        public int getBestMatchLength() {
            return bestMatchLength;
        }

        @Override
        protected void updateBestMatch(final int matchOffset, final int matchLength) {
            if (bestMatchLength < matchLength) {
                bestMatchLength = matchLength;
                bestMatchOffset = matchOffset;
            }
        }
    }

    private class RemoveOffset extends Offset {

        private RemoveOffset(int offset) {
            super(offset);
        }

        @Override
        public int compareTo(final Offset right) {
            final int result = super.compareTo(right);
            return result != 0 ? result : offset - right.offset;
        }
    }

    private static class LimitedInputStream extends InputStream {

        @NotNull
        private final InputStream decorated;
        private final int bytesToRead;
        private int bytesRead;

        private LimitedInputStream(@NotNull final InputStream decorated, final int bytesToRead) {
            this.decorated = decorated;
            this.bytesToRead = bytesToRead;
            bytesRead = 0;
        }

        @Override
        public int read() throws IOException {
            if (bytesRead >= bytesToRead) {
                return -1;
            }
            final int result = decorated.read();
            if (result >= 0) {
                ++bytesRead;
            }
            return result;
        }

        public int bytesRead() {
            return bytesRead;
        }
    }
}