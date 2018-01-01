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

public class InMemoryQuickSortOnInitIterable extends SortEngine.InMemorySortIterable {
    public InMemoryQuickSortOnInitIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
        super(source, comparator);
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<Entity>() {
            private List<Entity> src;
            private Entity[] tmp;
            private int current;

            @Override
            public boolean hasNext() {
                if (src == null) {
                    init();
                }
                return current < src.size();
            }

            @Override
            public Entity next() {
                if (src == null) {
                    init();
                }
                if (current >= src.size()) {
                    throw new NoSuchElementException();
                }
                return src.get(current++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void init() {
                src = new ArrayList<>();
                for (final Entity entity : source) {
                    src.add(entity);
                }
                tmp = new Entity[src.size()];
                qsort(0, tmp.length - 1);
            }

            /**
             * sort src[left..right]
             * @param left left
             * @param right right
             */
            public void qsort(int left, int right) {
                if (left >= right) {
                    return;
                }
                Entity median = src.get((left + right) / 2);
                int i = left;
                int toRight = 0;
                List<Entity> medians = new ArrayList<>();
                do {
                    while (i <= right && comparator.compare(src.get(i), median) < 0) {
                        if (toRight + medians.size() > 0) {
                            src.set(i - toRight - medians.size(), src.get(i));
                        }
                        i++;
                    }
                    // src[i] >= median
                    if (i <= right) {
                        Entity entity = src.get(i);
                        if (comparator.compare(entity, median) == 0) {
                            medians.add(entity);
                        } else {
                            tmp[toRight++] = entity;
                        }
                        i++;
                    }
                } while (i <= right);
                // i == right + 1
                int current = i - toRight - medians.size();
                for (Entity e : medians) {
                    src.set(current++, e);
                }
                for (int k = 0; k < toRight; k++) {
                    src.set(current++, tmp[k]);
                }
                qsort(left, right - toRight - medians.size());
                qsort(right - toRight + 1, right);
            }
        };
    }
}
