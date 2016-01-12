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
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.Entity;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class InMemoryMergeSortIterable extends SortEngine.InMemorySortIterable {
    public InMemoryMergeSortIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
        super(source, comparator);
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<Entity>() {
            private Entity[] src;
            private int size;
            private int height;
            private int size2;
            private int[] next;
            /**
             * need it only for hasNext()
             */
            private int current;

            @Override
            public boolean hasNext() {
                if (src == null) {
                    init();
                }
                return current < size;
            }

            @Override
            public Entity next() {
                if (src == null) {
                    init();
                }
                int segment = 1;
                // next[current] is index of the least remaining element on current segment
                // next[current] == -1 means minimum on current segment is not counted yet
                // next[current] == src.size() means current segment is exhausted
                while (next[1] < 0) {
                    segment <<= 1;
                    if (segment >= size2 || (next[segment] >= 0 && next[segment + 1] >= 0)) {
                        if ((next[segment + 1] >= size) || ((next[segment] < size) && (comparator.compare(src[next[segment]], src[next[segment + 1]]) <= 0))) {
                            next[segment >> 1] = next[segment];
                        } else {
                            next[segment >> 1] = next[segment + 1];
                        }
                        segment >>= 2;
                    } else if (next[segment] >= 0) {
                        segment++;
                    }
                }
                int r = next[1];
                if (r >= size) {
                    throw new NoSuchElementException();
                }
                next[r + size2] = size;
                for (int i = (r + size2) >> 1; i >= 1; i >>= 1) {
                    next[i] = -1;
                }
                current++;
                return src[r];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void init() {
                src = new Entity[1];
                for (final Entity entity : source) {
                    if (size >= src.length) {
                        src = Arrays.copyOf(src, src.length << 1);
                    }
                    src[size++] = entity;
                }
                height = 1;
                for (int i = size; i > 1; i = (i + 1) >> 1) {
                    height++;
                }
                next = new int[1 << height];
                size2 = 1 << (height - 1);
                for (int i = 0; i < size; i++) {
                    next[i + size2] = i;
                }
                for (int i = size2 + size; i < next.length; i++) {
                    next[i] = size;
                }
                for (int i = 1; i < size2; i++) {
                    next[i] = -1;
                }
                current = 0;
            }
        };
    }
}
