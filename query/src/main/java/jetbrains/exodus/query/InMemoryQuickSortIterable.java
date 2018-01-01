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

public class InMemoryQuickSortIterable extends SortEngine.InMemorySortIterable {
    public InMemoryQuickSortIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
        super(source, comparator);
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<Entity>() {
            private List<Entity> src;
            private List<Integer> left;
            private List<Integer> right;
            private List<Integer> medianStart;
            private List<Integer> medianEnd;
            private List<Entity> medians;
            private List<Entity> toRight;
            private int top;
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
                while (top >= 0 && right.get(top) < current) {
                    left.remove(top);
                    right.remove(top);
                    medianStart.remove(top);
                    medianEnd.remove(top);
                    top--;
                }
                // either current == 0 (and top == -1)
                // or     medianStart[top] <= current <= medianEnd[top]
                // or     current == medianEnd[top] + 1 and we should go deeper
                if (top < 0 || current > medianEnd.get(top)) {
                    do {
                        int l;
                        int r;
                        if (top < 0) {
                            l = 0;
                            r = src.size() - 1;
                        } else if (current == left.get(top)) {
                            l = left.get(top);
                            r = medianStart.get(top) - 1;
                        } else {
                            l = medianEnd.get(top) + 1;
                            r = right.get(top);
                        }

                        Entity median = src.get((l + r) / 2);
                        int i = l;
                        medians.clear();
                        toRight.clear();
                        do {
                            while (i <= r && comparator.compare(src.get(i), median) < 0) {
                                if (toRight.size() + medians.size() > 0) {
                                    src.set(i - toRight.size() - medians.size(), src.get(i));
                                }
                                i++;
                            }
                            // src[i] >= median
                            if (i <= r) {
                                Entity entity = src.get(i);
                                if (comparator.compare(entity, median) == 0) {
                                    medians.add(entity);
                                } else {
                                    toRight.add(entity);
                                }
                                i++;
                            }
                        } while (i <= r);
                        // i == r + 1
                        int current = i - toRight.size() - medians.size();
                        for (Entity e : medians) {
                            src.set(current++, e);
                        }
                        for (Entity e : toRight) {
                            src.set(current++, e);
                        }

                        left.add(l);
                        right.add(r);
                        medianStart.add(r + 1 - toRight.size() - medians.size());
                        medianEnd.add(r - toRight.size());
                        top++;
                    } while (current < medianStart.get(top));
                }
                return src.get(current++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void init() {
                src = new ArrayList<>();
                for (final Entity it : source) {
                    src.add(it);
                }
                left = new ArrayList<>();
                right = new ArrayList<>();
                medianStart = new ArrayList<>();
                medianEnd = new ArrayList<>();
                medians = new ArrayList<>();
                toRight = new ArrayList<>();
                top = -1;
            }
        };
    }
}
