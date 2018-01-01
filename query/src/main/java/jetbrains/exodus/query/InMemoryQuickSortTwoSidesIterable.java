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

public class InMemoryQuickSortTwoSidesIterable extends SortEngine.InMemorySortIterable {
    public InMemoryQuickSortTwoSidesIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
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
                int j = right;
                int toRight = 0;
                int toLeft = 0;
                List<Entity> leftMedians = new ArrayList<>();
                List<Entity> rightMedians = new ArrayList<>();
                do {
                    while (i <= j && comparator.compare(src.get(i), median) < 0) {
                        if (toRight + leftMedians.size() > 0) {
                            src.set(i - toRight - leftMedians.size(), src.get(i));
                        }
                        i++;
                    }
                    while (i <= j && comparator.compare(median, src.get(j)) < 0) {
                        if (toLeft + rightMedians.size() > 0) {
                            src.set(j + toLeft + rightMedians.size(), src.get(j));
                        }
                        j--;
                    }
                    // src[i] >= median and src[j] <= median
                    if (i <= j) {
                        Entity leftEntity = src.get(i);
                        if (comparator.compare(leftEntity, median) == 0) {
                            leftMedians.add(leftEntity);
                        } else {
                            tmp[toRight++] = leftEntity;
                        }
                        i++;
                    }
                    if (i <= j) {
                        Entity rightEntity = src.get(j);
                        if (comparator.compare(rightEntity, median) == 0) {
                            rightMedians.add(rightEntity);
                        } else {
                            tmp[right - toLeft++] = src.get(j);
                        }
                        j--;
                    }
                } while (i <= j);
                // i == j + 1
                int current = i - toRight - leftMedians.size();
                for (int k = right - toLeft + 1; k <= right; k++) {
                    src.set(current++, tmp[k]);
                }
                for (Entity e : leftMedians) {
                    src.set(current++, e);
                }
                for (int k = rightMedians.size() - 1; k >= 0; k--) {
                    src.set(current++, rightMedians.get(k));
                }
                for (int k = 0; k < toRight; k++) {
                    src.set(current++, tmp[k]);
                }
                qsort(left, i - toRight - leftMedians.size() + toLeft);
                qsort(current - toRight, right);
            }
        };
    }
}
