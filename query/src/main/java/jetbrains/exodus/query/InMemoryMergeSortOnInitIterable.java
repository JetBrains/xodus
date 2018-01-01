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
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.Entity;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class InMemoryMergeSortOnInitIterable extends SortEngine.InMemorySortIterable {
    public InMemoryMergeSortOnInitIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
        super(source, comparator);
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<Entity>() {
            private List<Entity>[] src;
            private int current;

            @Override
            public boolean hasNext() {
                if (src == null) {
                    init();
                }
                return current < src[0].size();
            }

            @Override
            public Entity next() {
                if (src == null) {
                    init();
                }
                if (current >= src[0].size()) {
                    throw new NoSuchElementException();
                }
                return src[0].get(current++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void init() {
                src = new ArrayList[]{new ArrayList<>(), null};
                for (final Entity entity : source) {
                    src[0].add(entity);
                }
                src[1] = new ArrayList<>(src[0]);
                msort(0, src[0].size() - 1, 0);
            }

            /**
             * sort interval left..right to src[c][left..right]
             * @param left left
             * @param right right
             * @param c c
             */
            public void msort(int left, int right, int c) {
                if (left >= right) {
                    for (int i = left; i <= right; i++) {
                        src[c].set(i, src[1 - c].get(i));
                    }
                    return;
                }
                int rStart = (left + right + 1) / 2;
                msort(left, rStart - 1, 1 - c);
                msort(rStart, right, 1 - c);
                int i = left;
                int j = rStart;
                for (int k = left; k <= right; k++) {
                    if ((j > right) || ((i < rStart) && (comparator.compare(src[1 - c].get(i), src[1 - c].get(j)) <= 0))) {
                        src[c].set(k, src[1 - c].get(i++));
                    } else {
                        src[c].set(k, src[1 - c].get(j++));
                    }
                }
            }
        };
    }
}
