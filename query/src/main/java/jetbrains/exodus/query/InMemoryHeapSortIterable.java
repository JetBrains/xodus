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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Cannot be used for sorting in memory since is not stable
 */
public class InMemoryHeapSortIterable extends SortEngine.InMemorySortIterable {

    private static Logger logger = LoggerFactory.getLogger(InMemoryHeapSortIterable.class);

    public InMemoryHeapSortIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
        super(source, comparator);
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<Entity>() {
            private int i;
            private int size;
            private List<Entity> heap;

            @Override
            public boolean hasNext() {
                if (heap == null) {
                    init();
                }
                return i < size;
            }

            @Override
            public Entity next() {
                if (heap == null) {
                    init();
                }
                if (i >= size) {
                    throw new NoSuchElementException();
                }
                ++i;
                siftDown(0);
                final Entity current = heap.get(0);
                heap.set(0, heap.remove(size - 1));
                return current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void init() {
                heap = new ArrayList<>();
                for (final Entity entity : source) {
                    heap.add(entity);
                }
                size = heap.size();
                if (size > 16) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("HeapSort called, size = " + size, new Exception());
                    }
                }
                for (int i = (size - 2) / 2; i > 0; i -= 1) {
                    siftDown(i);
                }
            }

            private void siftDown(int i) {
                final int count = heap.size();
                while (true) {
                    int j = (i * 2) + 1;
                    if (j >= count) {
                        break;
                    }
                    Entity child = heap.get(j);
                    if (j < count - 1) {
                        final Entity rightChild = heap.get(j + 1);
                        if (comparator.compare(child, rightChild) >= 0) {
                            child = rightChild;
                            j += 1;
                        }
                    }
                    final Entity parent = heap.get(i);
                    if (comparator.compare(child, parent) >= 0) {
                        break;
                    }
                    heap.set(i, child);
                    heap.set(j, parent);
                    i = j;
                }
            }
        };
    }
}
