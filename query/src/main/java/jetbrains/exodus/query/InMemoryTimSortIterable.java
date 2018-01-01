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

public class InMemoryTimSortIterable extends SortEngine.InMemorySortIterable {
    private static final int MIN_RUN_LENGTH = 32;

    public InMemoryTimSortIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
        super(source, comparator);
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<Entity>() {
            private List<Entity> src;
            private int runCount;
            private int nodeCount;
            /**
             * for leaves (runs) this is index of first element left
             * in particular, after init this is the leftmost element of the run
             * for other nodes from[] contains index of the minimal element calculated
             * or -1 if it's not calculated yet
             */
            private int[] from;
            private int[] len;
            private int[] left;
            private int[] right;
            private int current;
            private int head;

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
                computeMinimum();
                int r = from[head];
                markAsUsed(r);
                current++;
                return src.get(r);
            }

            private void computeMinimum() {
                ArrayList<Integer> stack = new ArrayList<>();
                stack.add(head);
                while (from[head] < 0) {
                    int node = stack.get(stack.size() - 1);
                    if (from[left[node]] < 0) {
                        stack.add(left[node]);
                    } else if (from[right[node]] < 0) {
                        stack.add(right[node]);
                    } else {
                        if (len[right[node]] <= 0 || (len[left[node]] > 0 && comparator.compare(src.get(from[left[node]]), src.get(from[right[node]])) <= 0)) {
                            from[node] = from[left[node]];
                            len[left[node]]--;
                        } else {
                            from[node] = from[right[node]];
                            len[right[node]]--;
                        }
                        stack.remove(stack.size() - 1);
                    }
                }
            }

            private void markAsUsed(int r) {
                int i = head;
                while (left[i] >= 0) {
                    from[i] = -1;
                    if (from[left[i]] == r) {
                        i = left[i];
                    } else {
                        i = right[i];
                    }
                }
                from[i]++;
                if (len[i] <= 0) {
                    from[i] = src.size();
                }
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
                int n = src.size();
                if (n == 0) {
                    return;
                }
                int minRunLength = n;
                int b = 0;
                while (minRunLength >= InMemoryTimSortIterable.MIN_RUN_LENGTH) {
                    b |= minRunLength;
                    minRunLength >>= 1;
                }
                minRunLength += b & 1;
                int maxRuns = 1 + (n + minRunLength - 1) / minRunLength;
                from = new int[maxRuns * 2 - 1];
                len = new int[maxRuns * 2 - 1];
                left = new int[maxRuns * 2 - 1];
                Arrays.fill(left, -1);
                right = new int[maxRuns * 2 - 1];
                ArrayList<Integer> stack = new ArrayList<>();
                runCount = 0;
                boolean growing = true;
                for (int i = 1; i < n; i++) {
                    // on the run
                    if ((comparator.compare(src.get(i - 1), src.get(i)) <= 0) == growing) {
                        continue;
                    }
                    // start of run
                    if (i == from[runCount] + 1) {
                        growing = !(growing);
                        continue;
                    }
                    // reverse if needed
                    if (!(growing)) {
                        reverse(i);
                    }
                    // insert several next elements into run to achieve run length minRunLength
                    for (; i < n && i - from[runCount] < minRunLength; i++) {
                        Entity t = src.get(i);
                        int j = i - 1;
                        for (; j >= from[runCount] && comparator.compare(src.get(j), t) > 0; j--) {
                            src.set(j + 1, src.get(j));
                        }
                        src.set(j + 1, t);
                    }
                    len[runCount] = i - from[runCount];
                    from[++runCount] = i;

                    stack.add(runCount - 1);
                    // collapse
                    while (true) {
                        int s = stack.size();
                        if (s >= 3 && len[stack.get(s - 3)] <= len[stack.get(s - 2)] + len[stack.get(s - 1)]) {
                            if (len[stack.get(s - 3)] < len[stack.get(s - 1)]) {
                                unite(stack, s - 3, maxRuns);
                            } else {
                                unite(stack, s - 2, maxRuns);
                            }
                        } else if (s >= 2 && len[stack.get(s - 2)] <= len[stack.get(s - 1)]) {
                            unite(stack, s - 2, maxRuns);
                        } else {
                            break;
                        }
                    }
                }
                // last run
                len[runCount] = n - from[runCount];
                if (len[runCount] > 0) {
                    // reverse if needed
                    if (!(growing)) {
                        reverse(n);
                    }
                    stack.add(runCount);
                    runCount++;
                }
                // collapse remaining
                for (int i = stack.size() - 2; i >= 0; i--) {
                    if (i > 0 && len[stack.get(i - 1)] < len[stack.get(i + 1)]) {
                        unite(stack, i - 1, maxRuns);
                    } else {
                        unite(stack, i, maxRuns);
                    }
                }
                // for inner nodes min value is unset
                for (int i = 0; i < nodeCount; i++) {
                    from[i + maxRuns] = -1;
                }
                head = stack.get(0);
            }

            private void reverse(int to) {
                for (int l = from[runCount], r = to - 1; l < r; l++, r--) {
                    Entity t = src.get(l);
                    src.set(l, src.get(r));
                    src.set(r, t);
                }
            }

            /**
             * replaces stack[p] and stack[p + 1] with a node pointing to them
             * @param stack stack
             * @param p p
             * @param shift shift
             */
            private void unite(ArrayList<Integer> stack, int p, int shift) {
                int node = nodeCount + shift;
                from[node] = from[stack.get(p)];
                len[node] = len[stack.get(p)] + len[stack.get(p + 1)];
                left[node] = stack.get(p);
                right[node] = stack.get(p + 1);
                stack.set(p, node);
                nodeCount++;
                stack.remove(p + 1);
            }
        };
    }
}
