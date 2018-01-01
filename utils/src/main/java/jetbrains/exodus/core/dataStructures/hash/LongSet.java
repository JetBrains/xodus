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
package jetbrains.exodus.core.dataStructures.hash;

import java.util.AbstractSet;
import java.util.Set;

public interface LongSet extends Set<Long> {
    LongSet EMPTY = new EmptySet();
    long[] EMPTY_ARRAY = new long[0];

    boolean contains(final long key);

    boolean add(final long key);

    boolean remove(final long key);

    @Override
    LongIterator iterator();

    long[] toLongArray();

    class EmptySet extends AbstractSet<Long> implements LongSet {

        @Override
        public boolean contains(long key) {
            return false;
        }

        @Override
        public boolean add(long key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(long key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LongIterator iterator() {
            return LongIterator.EMPTY;
        }

        @Override
        public long[] toLongArray() {
            return EMPTY_ARRAY;
        }

        @Override
        public int size() {
            return 0;
        }
    }
}
