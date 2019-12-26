/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.FixedLengthByteIterable;
import org.jetbrains.annotations.NotNull;

class LogAwareFixedLengthByteIterable extends FixedLengthByteIterable {

    LogAwareFixedLengthByteIterable(@NotNull final ByteIterableWithAddress source, final int offset, final int length) {
        super(source, offset, length);
    }

    @Override
    public int compareTo(final ByteIterable right) {
        return getSource().compareTo(offset, length, right);
    }

    @Override
    public ByteIterableWithAddress getSource() {
        return (ByteIterableWithAddress) super.getSource();
    }

    @Override
    protected ByteIterator getIterator() {
        if (length == 0) {
            return ByteIterable.EMPTY_ITERATOR;
        }
        final ByteIterator bi = source.iterator();
        bi.skip(offset);
        return new LogAwareFixedLengthByteIterator(bi, length);
    }

    private static class LogAwareFixedLengthByteIterator extends ByteIterator implements BlockByteIterator {

        private final ByteIterator si;
        private int bytesToRead;

        LogAwareFixedLengthByteIterator(@NotNull final ByteIterator si, final int length) {
            this.si = si;
            bytesToRead = length;
        }

        @Override
        public boolean hasNext() {
            return bytesToRead > 0 && si.hasNext();
        }

        @Override
        public byte next() {
            bytesToRead--;
            return si.next();
        }

        @Override
        public long skip(long bytes) {
            long result = si.skip(Math.min(bytes, bytesToRead));
            bytesToRead -= (int) result;
            return result;
        }

        @Override
        public int nextBytes(byte[] array, int off, int len) {
            final int bytesToRead = Math.min(len, this.bytesToRead);
            if (si instanceof BlockByteIterator) {
                final int result = ((BlockByteIterator) si).nextBytes(array, off, bytesToRead);
                this.bytesToRead -= result;
                return result;
            }
            for (int i = 0; i < bytesToRead; ++i) {
                array[off + i] = next();
            }
            return bytesToRead;
        }
    }
}
