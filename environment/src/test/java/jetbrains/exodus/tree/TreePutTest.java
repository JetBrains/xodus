/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.TestFor;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.IntHashSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public abstract class TreePutTest extends TreeBaseTest {

    @Test
    public void testTrivialGet() {
        tm = createMutableTree(false, 1);
        final long address = saveTree();
        t = openTree(address, false);
        assertNull(t.get(key(1)));
    }

    @Test
    public void testPutOverwriteWithoutDuplicates() {
        tm = createMutableTree(false, 1);

        tm.put(key("1"), value("1"));
        valueEquals("1", tm.get(key("1")));
        tm.put(key("1"), value("11"));
        valueEquals("11", tm.get(key("1")));
        assertEquals(1, tm.getSize());

        assertTrue(tm.hasKey(key("1")));
        assertFalse(tm.hasKey(key("2")));
        assertTrue(tm.hasPair(key("1"), value("11")));
        assertFalse(tm.hasPair(key("1"), value("1")));
    }

    @Test
    public void testPutOverwriteWithDuplicates() {
        tm = createMutableTree(true, 1);

        tm.put(key("1"), value("1"));
        valueEquals("1", tm.get(key("1")));
        assertTrue(tm.put(key("1"), value("11")));
        valueEquals("1", tm.get(key("1")));
        assertEquals(2, tm.getSize());
        assertFalse(tm.put(key("1"), value("11")));
    }

    @Test
    public void testAddNoOverwriteWithoutDuplicates() {
        tm = createMutableTree(false, 1);

        assertTrue(tm.add(key("1"), value("1")));
        valueEquals("1", tm.get(key("1")));
        assertFalse(tm.add(key("1"), value("11")));
        valueEquals("1", tm.get(key("1")));
        assertEquals(1, tm.getSize());

        assertTrue(tm.hasKey(key("1")));
        assertFalse(tm.hasKey(key("2")));
        assertFalse(tm.hasPair(key("1"), value("11")));
        assertTrue(tm.hasPair(key("1"), value("1")));
    }

    @Test
    public void testPutKeysWithSamePrefix() {
        tm = createMutableTree(false, 1);

        StringBuilder key = new StringBuilder();
        for (int i = 0; i < 100; ++i) {
            key.append('1');
            assertTrue(tm.add(key(key.toString()), value(Integer.toString(i))));
        }

        key.setLength(0);
        for (int i = 0; i < 100; ++i) {
            key.append('1');
            valueEquals(Integer.toString(i), tm.get(key(key.toString())));
        }
    }

    @Test
    public void testPutAllOverwriteWithoutDuplicates() {
        tm = createMutableTree(false, 1);

        final int count = 1000;

        for (int i = 0; i < count; ++i) {
            tm.put(key(Integer.toString(i)), value(Integer.toString(i)));
        }
        for (int i = 0; i < count; ++i) {
            tm.put(key(Integer.toString(i)), value(Integer.toString(count - i)));
        }

        Assert.assertEquals(count, tm.getSize());
        for (int i = 0; i < count; ++i) {
            valueEquals(Integer.toString(count - i), tm.get(key(Integer.toString(i))));
        }
    }

    @Test
    public void testPutReopen() {
        tm = createMutableTree(false, 1);
        tm.put(key("1"), value("1"));
        final long address = saveTree();

        reopen();

        t = openTree(address, false);
        valueEquals("1", t.get(key("1")));
    }

    @Test
    public void testPutReopen2() {
        tm = createMutableTree(false, 1);
        tm.put(key("11"), value("1"));
        final long address = saveTree();

        reopen();

        t = openTree(address, false);
        valueEquals("1", t.get(key("11")));
    }

    @Test
    public void testPutReopen3() {
        tm = createMutableTree(false, 1);
        tm.put(key("1"), value("1"));
        tm.put(key("2"), value("1"));
        final long address = saveTree();

        reopen();

        t = openTree(address, false);
        valueEquals("1", t.get(key("1")));
        valueEquals("1", t.get(key("2")));
    }

    @Test
    public void testPutReopen4() {
        tm = createMutableTree(false, 1);
        tm.put(key("1"), value("1"));
        tm.put(key("2"), value("1"));
        long address = saveTree();

        reopen();

        t = openTree(address, false);
        valueEquals("1", t.get(key("1")));
        valueEquals("1", t.get(key("2")));

        tm = t.getMutableCopy();
        tm.put(key("2"), value("2"));
        address = saveTree();

        reopen();

        t = openTree(address, false);
        valueEquals("1", t.get(key("1")));
        valueEquals("2", t.get(key("2")));
    }

    @Test
    public void testPutRight() {
        tm = createMutableTree(false, 1);
        tm.put(key("1"), value("1"));
        tm.put(key("2"), value("1"));
        TestUtil.runWithExpectedException(() -> tm.putRight(key("1"), value("1")), IllegalArgumentException.class);
        TestUtil.runWithExpectedException(() -> tm.putRight(key("2"), value("2")), IllegalArgumentException.class);
        tm.putRight(key("3"), value("3"));
        TestUtil.runWithExpectedException(() -> tm.putRight(key("1"), value("1")), IllegalArgumentException.class);
        TestUtil.runWithExpectedException(() -> tm.putRight(key("2"), value("1")), IllegalArgumentException.class);
    }

    @Test
    public void testPutRight3() {
        tm = createMutableTree(false, 1);
        final int count = 10000;
        for (int i = 0; i < count; ++i) {
            tm.putRight(IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
            if (i % 32 == 0) {
                final long address = saveTree();
                tm = openTree(address, false).getMutableCopy();
            }
        }
        final long address = saveTree();
        tm = openTree(address, false).getMutableCopy();
        final ITreeCursor cursor = tm.openCursor();
        for (int i = 0; i < count; ++i) {
            Assert.assertTrue(cursor.getNext());
            final ByteIterable key = cursor.getKey();
            final ByteIterable value = cursor.getValue();
            assertEquals(0, key.compareTo(value));
            Assert.assertEquals(i, IntegerBinding.readCompressed(key.iterator()));
        }
        cursor.close();
    }

    @Test
    public void testPutRight2() {
        tm = createMutableTree(false, 1);
        final StringBuilder key = new StringBuilder();
        final int count = 1000;
        for (int i = 0; i < count; ++i) {
            key.append('1');
            assertTrue(tm.add(key(key.toString()), value(Integer.toString(i))));
        }
        key.setLength(0);
        for (int i = 0; i < count - 1; ++i) {
            key.append('1');
            TestUtil.runWithExpectedException(() -> tm.putRight(key(key.toString()), value("0")), IllegalArgumentException.class);
        }
    }

    @Test
    public void xd_329() {
        tm = createMutableTree(false, 1);
        final long count = 17;
        for (long i = 0; i < count; ++i) {
            tm.putRight(LongBinding.longToCompressedEntry(i), LongBinding.longToCompressedEntry(i));
            final long address = saveTree();
            reopen();
            tm = openTree(address, false).getMutableCopy();
        }
        tm.putRight(LongBinding.longToCompressedEntry(count), LongBinding.longToCompressedEntry(count));
    }

    @Test
    public void xd_329_with_ints() {
        tm = createMutableTree(false, 1);
        final int count = 33;
        for (int i = 0; i < count; ++i) {
            tm.putRight(IntegerBinding.intToCompressedEntry(i), IntegerBinding.intToCompressedEntry(i));
            final long address = saveTree();
            reopen();
            tm = openTree(address, false).getMutableCopy();
        }
        tm.putRight(IntegerBinding.intToCompressedEntry(count), IntegerBinding.intToCompressedEntry(count));
    }

    @Test
    public void testPutKeysWithSamePrefixReopen() {
        tm = createMutableTree(false, 1);

        StringBuilder key = new StringBuilder();
        final int count = 500;
        for (int i = 0; i < count; ++i) {
            key.append('1');
            assertTrue(tm.add(key(key.toString()), value(Integer.toString(i))));
        }

        long address = saveTree();
        reopen();
        t = openTree(address, false);

        key.setLength(0);
        for (int i = 0; i < count; ++i) {
            key.append('1');
            valueEquals(Integer.toString(i), t.get(key(key.toString())));
        }
    }

    @Test
    public void testPutRandomWithoutDuplicates() {
        tm = createMutableTree(false, 1);

        final IntHashMap<String> map = new IntHashMap<>();
        final int count = 200000;

        TestUtil.time("put()", () -> {
            for (int i = 0; i < count; ++i) {
                final int key = Math.abs(RANDOM.nextInt());
                final String value = Integer.toString(i);
                tm.put(key(Integer.toString(key)), value(value));
                map.put(key, value);
            }
        });

        Assert.assertEquals(map.size(), tm.getSize());
        TestUtil.time("get()", () -> {
            for (final Map.Entry<Integer, String> entry : map.entrySet()) {
                final Integer key = entry.getKey();
                final String value = entry.getValue();
                valueEquals(value, tm.get(key(Integer.toString(key))));
            }
        });
    }

    @Test
    public void testPutRandomWithoutDuplicates2() {
        tm = createMutableTree(false, 1);

        final IntHashMap<String> map = new IntHashMap<>();
        final int count = 200000;

        TestUtil.time("put()", () -> {
            for (int i = 0; i < count; ++i) {
                final int key = Math.abs(RANDOM.nextInt());
                final String value = Integer.toString(i);
                tm.put(key(Integer.toString(key)), value(value));
                map.put(key, value);
            }
        });

        final long address = saveTree();
        reopen();
        t = openTree(address, false);

        Assert.assertEquals(map.size(), t.getSize());

        TestUtil.time("get()", () -> {
            for (final Map.Entry<Integer, String> entry : map.entrySet()) {
                final Integer key = entry.getKey();
                final String value = entry.getValue();
                valueEquals(value, t.get(key(Integer.toString(key))));
            }
        });
    }

    @Test
    public void testPutRightRandomWithoutDuplicates() {
        tm = createMutableTree(false, 1);

        final IntHashMap<String> map = new IntHashMap<>();
        final int count = 99999;

        TestUtil.time("putRight()", () -> {
            for (int i = 0; i < count; ++i) {
                final String value = Integer.toString(i);
                tm.putRight(key(i), value(value));
                map.put(i, value);
            }
        });

        final long address = saveTree();
        reopen();
        t = openTree(address, false);

        Assert.assertEquals(map.size(), t.getSize());

        TestUtil.time("get()", () -> {
            for (final Map.Entry<Integer, String> entry : map.entrySet()) {
                final Integer key = entry.getKey();
                final String value = entry.getValue();
                valueEquals(value, t.get(key(key)));
            }
        });
    }

    @Test
    public void testAddRandomWithoutDuplicates() {
        tm = createMutableTree(false, 1);

        final IntHashMap<String> map = new IntHashMap<>();
        final int count = 50000;

        TestUtil.time("add()", () -> {
            for (int i = 0; i < count; ++i) {
                final int key = Math.abs(RANDOM.nextInt());
                final String value = Integer.toString(i);
                assertEquals(!map.containsKey(key), tm.add(key(Integer.toString(key)), value(value)));

                if (!map.containsKey(key)) {
                    map.put(key, value);
                }
            }
        });

        final long address = saveTree();
        reopen();
        t = openTree(address, false);

        Assert.assertEquals(map.size(), t.getSize());

        TestUtil.time("get()", () -> {
            for (final Map.Entry<Integer, String> entry : map.entrySet()) {
                final Integer key = entry.getKey();
                final String value = entry.getValue();
                valueEquals(value, t.get(key(Integer.toString(key))));
            }
        });

        tm = t.getMutableCopy();

        TestUtil.time("Failing add()", () -> {
            for (final Map.Entry<Integer, String> entry : map.entrySet()) {
                final Integer key = entry.getKey();
                final String value = entry.getValue();
                assertFalse(tm.add(key(Integer.toString(key)), value(value)));
            }
        });
    }

    @Test
    @TestFor(issue = "XD-539")
    public void createHugeTree() {
        if (Runtime.getRuntime().maxMemory() < 4000000000L) {
            return;
        }

        tm = createMutableTree(false, 1);

        final IntHashSet set = new IntHashSet();
        final int count = 20000;
        final StringBuilder builder = new StringBuilder("value");

        TestUtil.time("put()", () -> {
            for (int i = 0; i < count; ++i) {
                tm.put(key(Integer.toString(i)), value(builder.toString()));
                set.add(i);
                builder.append(i);
            }
        });

        final long address = saveTree();
        System.out.println("Log size: " + tm.getLog().getHighAddress());
        reopen();
        t = openTree(address, false);

        Assert.assertEquals(set.size(), t.getSize());

        TestUtil.time("get()", () -> {
            for (Integer i : set) {
                assertTrue(t.hasKey(key(Integer.toString(i))));
            }
        });
    }
}
