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
package jetbrains.exodus;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"CovariantCompareTo"})
public interface ByteIterable extends Comparable<ByteIterable> {

    byte[] EMPTY_BYTES = {};

    ByteIterator EMPTY_ITERATOR = new ByteIterator() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public byte next() {
            return (byte) 0;
        }

        @Override
        public long skip(long length) {
            return 0;
        }
    };

    ByteIterable EMPTY = new ByteIterable() {

        @Override
        public ByteIterator iterator() {
            return EMPTY_ITERATOR;
        }

        @Override
        public int compareTo(@NotNull ByteIterable right) {
            return right.iterator().hasNext() ? -1 : 0;
        }

        @SuppressWarnings({"ReturnOfCollectionOrArrayField"})
        @Override
        public byte[] getBytesUnsafe() {
            return EMPTY_BYTES;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public ByteIterable subIterable(int offset, int length) {
            return this;
        }

        @Override
        public String toString() {
            return "[ByteIterable.EMPTY]";
        }
    };

    ByteIterator iterator();

    /**
     * Returns content of byte iterable.
     * May return array with len > getLength(), but client may use only first getLength() bytes.
     *
     * @return bytes array
     */
    byte[] getBytesUnsafe();

    int getLength();

    @NotNull
    ByteIterable subIterable(final int offset, final int length);
}
