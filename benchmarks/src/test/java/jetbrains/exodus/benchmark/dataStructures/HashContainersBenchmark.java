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
package jetbrains.exodus.benchmark.dataStructures;

import jetbrains.exodus.core.dataStructures.hash.*;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class HashContainersBenchmark {

    @Test
    public void benchmarkHashMapGet() {

        long started;

        final Map<Integer, String> map = new java.util.HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashMap took " + (System.currentTimeMillis() - started));

        final HashMap<Integer, String> tested = new HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("100 000 000 lookups in HashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkHashMapGetMissingKeys() {

        long started;

        final Map<Integer, String> map = new java.util.HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashMap took " + (System.currentTimeMillis() - started));

        final Map<Integer, String> tested = new HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in HashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkHashSetContains() {

        long started;

        final Set<Integer> set = new java.util.HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet took " + (System.currentTimeMillis() - started));

        final HashSet<Integer> tested = new HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in HashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkHashSetContainsMissingKeys() {

        long started;

        final Set<Integer> set = new java.util.HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet took " + (System.currentTimeMillis() - started));

        final HashSet<Integer> tested = new HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in HashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkHashSetContainsMissingObjectKeys() {

        long started;

        final Set<Object> set = new java.util.HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(new Object());
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 500; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(new Object());
            }
        }
        System.out.println("50 000 000 lookups in java.util.HashSet took " + (System.currentTimeMillis() - started));

        final HashSet<Object> tested = new HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            tested.add(new Object());
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 500; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(new Object());
            }
        }
        System.out.println("50 000 000 lookups in HashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkIntHashMapGet() {

        long started;

        final Map<Integer, String> map = new java.util.HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashMap took " + (System.currentTimeMillis() - started));

        final IntHashMap<String> tested = new IntHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("100 000 000 lookups in IntHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkIntHashMapGetMissingKeys() {

        long started;

        final Map<Integer, String> map = new java.util.HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashMap took " + (System.currentTimeMillis() - started));

        final IntHashMap<String> tested = new IntHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in IntHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkIntHashSetContains() {

        long started;

        final Set<Integer> set = new java.util.HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet<Integer> took " + (System.currentTimeMillis() - started));

        final IntHashSet tested = new IntHashSet();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in IntHashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkIntHashSetContainsMissingKeys() {

        long started;

        final Set<Integer> set = new java.util.HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet<Integer> took " + (System.currentTimeMillis() - started));

        final IntHashSet tested = new IntHashSet();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in IntHashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkIntLinkedHashMapGet() {

        long started;

        final Map<Integer, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final IntLinkedHashMap<String> tested = new IntLinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("100 000 000 lookups in IntLinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkIntLinkedHashMapGetMissingKeys() {

        long started;

        final Map<Integer, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final IntLinkedHashMap<String> tested = new IntLinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in IntLinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkIntLinkedHashMapLRU() {

        long started;

        final Map<Integer, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 200; ++i) {
            for (int j = 0; j < 100000; ++j) {
                final String v = map.remove(j);
                map.put(j, v);
            }
        }
        System.out.println("20 000 000 LRU lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final IntLinkedHashMap<String> tested = new IntLinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 200; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("20 000 000 lookups in IntLinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLinkedHashMapGet() {

        long started;

        final Map<Integer, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("100 000 000 lookups in LinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLinkedHashMapGetMissingKeys() {

        long started;

        final Map<Integer, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                map.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in LinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLinkedHashMapLRU() {

        long started;

        final Map<Integer, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            map.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 200; ++i) {
            for (int j = 0; j < 100000; ++j) {
                final String v = map.remove(j);
                map.put(j, v);
            }
        }
        System.out.println("20 000 000 LRU lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 200; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("20 000 000 lookups in LinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLinkedHashSetContains() {

        long started;

        final Set<Integer> set = new java.util.LinkedHashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet took " + (System.currentTimeMillis() - started));

        final LinkedHashSet<Integer> tested = new LinkedHashSet<>();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in LinkedHashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLinkedHashSetContainsMissingKeys() {

        long started;

        final Set<Integer> set = new java.util.LinkedHashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet took " + (System.currentTimeMillis() - started));

        final LinkedHashSet<Integer> tested = new LinkedHashSet<>();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in LinkedHashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongHashMapGet() {

        long started;

        final Map<Long, String> map = new java.util.HashMap<>();
        for (long i = 0; i < 100000; ++i) {
            map.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                map.get(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashMap took " + (System.currentTimeMillis() - started));

        final LongHashMap<String> tested = new LongHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            tested.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("100 000 000 lookups in LongHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongHashMapGetMissingKeys() {

        long started;

        final Map<Long, String> map = new java.util.HashMap<>();
        for (long i = 0; i < 100000; ++i) {
            map.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                map.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashMap took " + (System.currentTimeMillis() - started));

        final LongHashMap<String> tested = new LongHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            tested.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                tested.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in LongHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongHashSetContains() {

        long started;

        final Set<Long> set = new java.util.HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i + 100000000000L);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                set.contains(j + 100000000000L);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet<Long> took " + (System.currentTimeMillis() - started));

        final LongHashSet tested = new LongHashSet();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i + 100000000000L);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j + 100000000000L);
            }
        }
        System.out.println("100 000 000 lookups in LongHashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongHashSetContainsMissingKeys() {

        long started;

        final Set<Long> set = new java.util.HashSet<>();
        for (int i = 0; i < 100000; ++i) {
            set.add(i + 100000000000L);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                set.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet<Long> took " + (System.currentTimeMillis() - started));

        final LongHashSet tested = new LongHashSet();
        for (int i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in LongHashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongLinkedHashMapGet() {

        long started;

        final Map<Long, String> map = new java.util.LinkedHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            map.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                map.get(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final LongLinkedHashMap<String> tested = new LongLinkedHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            tested.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("100 000 000 lookups in LongLinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongLinkedHashMapGetMissingKeys() {

        long started;

        final Map<Long, String> map = new java.util.LinkedHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            map.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                map.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final LongLinkedHashMap<String> tested = new LongLinkedHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            tested.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in LongLinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongLinkedHashMapLRU() {

        long started;

        final Map<Long, String> map = new java.util.LinkedHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            map.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 200; ++i) {
            for (long j = 0; j < 100000; ++j) {
                final String v = map.remove(j);
                map.put(j, v);
            }
        }
        System.out.println("20 000 000 LRU lookups in java.util.LinkedHashMap took " + (System.currentTimeMillis() - started));

        final LongLinkedHashMap<String> tested = new LongLinkedHashMap<>();
        for (long i = 0; i < 100000; ++i) {
            tested.put(i, Long.toString(i));
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 200; ++i) {
            for (int j = 0; j < 100000; ++j) {
                tested.get(j);
            }
        }
        System.out.println("20 000 000 lookups in LongLinkedHashMap took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongLinkedHashSetContains() {

        long started;

        final Set<Long> set = new java.util.LinkedHashSet<>();
        for (long i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                set.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet took " + (System.currentTimeMillis() - started));

        final LongLinkedHashSet tested = new LongLinkedHashSet();
        for (long i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                tested.contains(j);
            }
        }
        System.out.println("100 000 000 lookups in LinkedHashSet took " + (System.currentTimeMillis() - started));
    }

    @Test
    public void benchmarkLongLinkedHashSetContainsMissingKeys() {

        long started;

        final Set<Long> set = new java.util.LinkedHashSet<>();
        for (long i = 0; i < 100000; ++i) {
            set.add(i);
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                set.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in java.util.HashSet took " + (System.currentTimeMillis() - started));

        final LongLinkedHashSet tested = new LongLinkedHashSet();
        for (long i = 0; i < 100000; ++i) {
            tested.add(i);
        }
        started = System.currentTimeMillis();
        for (long i = 0; i < 1000; ++i) {
            for (long j = 0; j < 100000; ++j) {
                tested.contains(j + 1000000000);
            }
        }
        System.out.println("100 000 000 lookups in LinkedHashSet took " + (System.currentTimeMillis() - started));
    }
}
