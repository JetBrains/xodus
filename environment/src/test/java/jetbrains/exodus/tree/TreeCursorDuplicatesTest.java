/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.tree;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public abstract class TreeCursorDuplicatesTest extends TreeBaseTest {

    List<INode> values = new ArrayList<>();
    Set<INode> valuesNoDup = new LinkedHashSet<>();

    @Before
    public void prepareTree() {
        tm = createMutableTree(true, 1);

        values.add(kv(1, "v1"));
        values.add(kv(2, "v2"));
        values.add(kv(5, "v51"));
        values.add(kv(5, "v52"));
        values.add(kv(5, "v53"));
        values.add(kv(7, "v7"));
        values.add(kv(8, "v8"));
        values.add(kv(9, "v9"));
        values.add(kv(10, "v10"));
        values.add(kv(11, "v11"));
        values.add(kv(12, "v12"));

        for (INode ln : values) {
            getTreeMutable().put(ln);
            valuesNoDup.add(ln);
        }
    }

    @Test
    public void testInitialState() {
        final TreeAwareRunnable initial = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();
                assertFalse(c.getKey().iterator().hasNext());
                assertFalse(c.getValue().iterator().hasNext());
            }
        };

        initial.run();
        long a = saveTree();
        initial.run();
        reopen();
        initial.setTree(openTree(a, true));
        initial.run();
    }

    @Test
    public void testCount() {
        final TreeAwareRunnable count = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                assertEquals(value("v1"), c.getSearchKey(key(1)));
                assertEquals(1, c.count());

                assertEquals(value("v51"), c.getSearchKey(key(5)));
                assertEquals(3, c.count());

                assertEquals(value("v12"), c.getSearchKey(key(12)));
                assertEquals(1, c.count());
            }
        };

        count.run();
        long a = saveTree();
        count.run();
        reopen();
        count.setTree(openTree(a, true));
        count.run();
    }

    @Test
    public void testGetNext() {
        final TreeAwareRunnable getNext = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                for (INode ln : values) {
                    assertTrue(c.getNext());
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }

                assertFalse(c.getNext());
            }
        };

        getNext.run();
        long a = saveTree();
        getNext.run();
        reopen();
        getNext.setTree(openTree(a, true));
        getNext.run();
    }

    @Test
    public void testGetNextDup() {
        final TreeAwareRunnable getNextDup = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                assertEquals(value("v1"), c.getSearchKey(key(1)));
                assertFalse(c.getNextDup());

                c.getSearchKey(key(5)); // 51
                assertTrue(c.getNextDup()); // 52
                assertTrue(c.getNextDup()); // 53
                assertFalse(c.getNextDup());

                c.getSearchKey(key(12));
                assertFalse(c.getNextDup());
            }
        };

        getNextDup.run();
        long a = saveTree();
        getNextDup.run();
        reopen();
        getNextDup.setTree(openTree(a, true));
        getNextDup.run();
    }

    @Test
    public void testGetNextDup2() {
        Cursor c = tm.openCursor();

        assertEquals(value("v1"), c.getSearchKey(key(1)));
        assertFalse(c.getNextDup());

        c.getSearchKey(key(5)); // 51
        c.getNextDup(); // 52
        c.getNextDup(); // 53
        c.deleteCurrent();
        assertFalse(c.getNextDup());
    }

    @Test
    public void testGetNextNoDup() {
        final TreeAwareRunnable getNextNoDup = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                for (INode ln : valuesNoDup) {
                    assertTrue(c.getNextNoDup());
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }

                assertFalse(c.getNextNoDup());
            }
        };

        getNextNoDup.run();
        long a = saveTree();
        getNextNoDup.run();
        reopen();
        getNextNoDup.setTree(openTree(a, true));
        getNextNoDup.run();
    }

    @Test
    public void testGetNextNoDup2() {
        Cursor c = tm.openCursor();

        assertEquals(value("v1"), c.getSearchKey(key(1)));
        assertFalse(c.getNextDup());

        c.getSearchKey(key(5)); // 51
        c.deleteCurrent();
        assertTrue(c.getNextNoDup());

        assertEquals(key(7), c.getKey());
        assertEquals(value("v7"), c.getValue());
    }

    @Test
    public void testGetSearchKey() {
        final TreeAwareRunnable getSearchKey = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                for (INode ln : valuesNoDup) {
                    assertEquals(ln.getValue(), c.getSearchKey(ln.getKey()));
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }

                assertNull(c.getSearchKey(key(0)));
                assertNull(c.getSearchKey(key(4)));
                assertNull(c.getSearchKey(key(13)));
                // prev state due to failed search
                assertEquals(values.get(values.size() - 1).getValue(), c.getValue());
                assertEquals(values.get(values.size() - 1).getKey(), c.getKey());
            }
        };

        getSearchKey.run();
        long a = saveTree();
        getSearchKey.run();
        reopen();
        getSearchKey.setTree(openTree(a, true));
        getSearchKey.run();
    }

    @Test
    public void testGetSearchBoth() {
        final TreeAwareRunnable getSearchBoth = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                for (INode ln : values) {
                    assertTrue(c.getSearchBoth(ln.getKey(), ln.getValue()));
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }

                assertFalse(c.getSearchBoth(key(0), value("v1")));
                assertFalse(c.getSearchBoth(key(4), value("v1")));
                assertFalse(c.getSearchBoth(key(13), value("v1")));
                // prev state due to failed search
                assertEquals(values.get(values.size() - 1).getValue(), c.getValue());
                assertEquals(values.get(values.size() - 1).getKey(), c.getKey());
            }
        };

        getSearchBoth.run();
        long a = saveTree();
        getSearchBoth.run();
        reopen();
        getSearchBoth.setTree(openTree(a, true));
        getSearchBoth.run();
    }

    @Test
    public void testGetSearchKeyRange1() {
        final TreeAwareRunnable getSearchKeyRange = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                for (INode ln : valuesNoDup) {
                    assertEquals(ln.getValue(), c.getSearchKeyRange(ln.getKey()));
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }
            }
        };

        getSearchKeyRange.run();
        long a = saveTree();
        getSearchKeyRange.run();
        reopen();
        getSearchKeyRange.setTree(openTree(a, true));
        getSearchKeyRange.run();
    }

    @Test
    public void testGetSearchKeyRange2() {
        final TreeAwareRunnable getSearchKeyRange = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                assertEquals(value("v1"), c.getSearchKeyRange(key(0)));
                assertEquals(key(1), c.getKey());

                assertEquals(value("v51"), c.getSearchKeyRange(key(3)));
                assertEquals(key(5), c.getKey());

                assertTrue(c.getNextDup());
                assertEquals(value("v52"), c.getValue());
                assertEquals(key(5), c.getKey());

                assertTrue(c.getNextDup());
                assertEquals(value("v53"), c.getValue());
                assertEquals(key(5), c.getKey());

                assertNull(c.getSearchKeyRange(key(13)));
                // cursor keep prev pos
                assertEquals(value("v53"), c.getValue());
                assertEquals(key(5), c.getKey());
            }
        };

        getSearchKeyRange.run();
        long a = saveTree();
        getSearchKeyRange.run();
        reopen();
        getSearchKeyRange.setTree(openTree(a, true));
        getSearchKeyRange.run();
    }

    @Test
    public void testGetSearchKeyRange3() {
        tm = createMutableTree(true, 1);

        getTreeMutable().put(kv(1, "v1"));
        getTreeMutable().put(kv(2, "v2"));
        getTreeMutable().put(kv(3, "v3"));
        getTreeMutable().put(kv(5, "v51"));
        getTreeMutable().put(kv(5, "v52"));
        getTreeMutable().put(kv(5, "v53"));
        getTreeMutable().put(kv(7, "v7"));
        getTreeMutable().put(kv(8, "v8"));

        // assertMatches(getTreeMutable(), IP(BP(3), BP(3)));

        final TreeAwareRunnable getSearchKeyRange = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                assertEquals(value("v51"), c.getSearchKeyRange(key(4)));
                assertEquals(key(5), c.getKey());
            }
        };

        getSearchKeyRange.run();
        long a = saveTree();
        getSearchKeyRange.run();
        reopen();
        getSearchKeyRange.setTree(openTree(a, true));
        getSearchKeyRange.run();
    }

    @Test
    public void testGetSearchKeyRange4() {
        tm = createMutableTree(true, 1);

        getTreeMutable().put(kv(1, "v1"));
        getTreeMutable().put(kv(2, "v2"));
        getTreeMutable().put(kv(3, "v3"));
        getTreeMutable().put(kv(5, "v51"));
        getTreeMutable().put(kv(5, "v52"));
        getTreeMutable().put(kv(5, "v53"));
        getTreeMutable().put(kv(7, "v7"));
        getTreeMutable().put(kv(8, "v8"));

        try (ITreeCursor cursor = getTreeMutable().openCursor()) {
            cursor.getSearchKeyRange(key(6));
            assertEquals(value("v7"), cursor.getValue());
            assertTrue(cursor.getNext());
            assertEquals(value("v8"), cursor.getValue());
            assertFalse(cursor.getNext());
        }
    }


    @Test
    public void testGetSearchBothRange1() {
        final TreeAwareRunnable getSearchBothRange = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                for (INode ln : values) {
                    assertEquals(ln.getValue(), c.getSearchBothRange(ln.getKey(), ln.getValue()));
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }
            }
        };

        getSearchBothRange.run();
        long a = saveTree();
        getSearchBothRange.run();
        reopen();
        getSearchBothRange.setTree(openTree(a, true));
        getSearchBothRange.run();
    }

    @Test
    public void testGetSearchBothRange2() {
        final TreeAwareRunnable getSearchBothRange = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();
                // miss
                assertNull(c.getSearchBothRange(key(0), value("v1")));

                // found
                assertEquals(value("v1"), c.getSearchBothRange(key(1), value("v0")));
                assertEquals(key(1), c.getKey());
                assertEquals(value("v1"), c.getValue());

                // miss
                assertNull(c.getSearchBothRange(key(2), value("v21")));
                // check keep prev state
                assertEquals(key(1), c.getKey());

                assertEquals(value("v51"), c.getSearchBothRange(key(5), value("v50")));
                assertEquals(key(5), c.getKey());
                assertEquals(value("v51"), c.getSearchBothRange(key(5), value("v51")));
                assertEquals(key(5), c.getKey());
                assertEquals(value("v53"), c.getSearchBothRange(key(5), value("v521")));
                assertEquals(key(5), c.getKey());

                assertNull(c.getSearchBothRange(key(5), value("v54")));
                assertEquals(value("v53"), c.getValue());
            }
        };

        getSearchBothRange.run();
        long a = saveTree();
        getSearchBothRange.run();
        reopen();
        getSearchBothRange.setTree(openTree(a, true));
        getSearchBothRange.run();
    }

    @Test
    public void testGetSearchBothRange3() {
        tm = createMutableTree(true, 1);

        getTreeMutable().put(kv(5, "v51"));
        getTreeMutable().put(kv(5, "v52"));
        getTreeMutable().put(kv(6, "v61"));
        getTreeMutable().put(kv(6, "v62"));

        // assertMatches(getTreeMutable(), IP(BP(3), BP(3)));

        final TreeAwareRunnable getSearchKeyRange = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                assertEquals(value("v51"), c.getSearchKeyRange(key(4)));
                assertEquals(key(5), c.getKey());
                assertNull(c.getSearchBothRange(key(5), value("v54")));
                assertEquals(key(5), c.getKey()); // key unchanged
            }
        };

        getSearchKeyRange.run();
        long a = saveTree();
        getSearchKeyRange.run();
        reopen();
        getSearchKeyRange.setTree(openTree(a, true));
        getSearchKeyRange.run();
    }

    @Test
    public void testGetPrev() {
        final TreeAwareRunnable getPrev = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                final ListIterator<INode> itr = values.listIterator(values.size());
                while (itr.hasPrevious()) {
                    INode ln = itr.previous();
                    assertTrue(c.getPrev());
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }
                assertFalse(c.getPrev());
            }
        };

        getPrev.run();
        long a = saveTree();
        getPrev.run();
        reopen();
        getPrev.setTree(openTree(a, true));
        getPrev.run();
    }

    @Test
    public void testGetPrev2() {
        tm = createMutableTree(true, 1);
        values.clear();
        values.add(kv(0, "v0"));
        values.add(kv(0, "v1"));
        values.add(kv(1, "v1"));
        for (INode ln : values) {
            getTreeMutable().put(ln);
            valuesNoDup.add(ln);
        }

        final TreeAwareRunnable getPrev = new TreeAwareRunnable(getTreeMutable()) {
            @Override
            public void run() {
                Cursor c = _t.openCursor();

                final ListIterator<INode> itr = values.listIterator(values.size());
                while (itr.hasPrevious()) {
                    INode ln = itr.previous();
                    assertTrue(c.getPrev());
                    assertEquals(ln.getValue(), c.getValue());
                    assertEquals(ln.getKey(), c.getKey());
                }
                assertFalse(c.getPrev());
            }
        };

        getPrev.run();
        long a = saveTree();
        getPrev.run();
        reopen();
        getPrev.setTree(openTree(a, true));
        getPrev.run();
    }

    @Test
    public void test_xd_347_like() {
        tm = createMutableTree(true, 1);
        final int count = 20000;
        long value = 0;
        final LongHashMap<LongHashSet> values = new LongHashMap<>();
        final Random rnd = new Random();
        for (int i = 0; i < count; ++i, ++value) {
            if (i > count / 2) {
                final Pair<Long, LongHashSet>[] pair = new Pair[1];
                values.forEachEntry((ObjectProcedure<Map.Entry<Long, LongHashSet>>) object -> {
                    pair[0] = new Pair<>(object.getKey(), object.getValue());
                    return false;
                });
                final Pair<Long, LongHashSet> p = pair[0];
                final LongHashSet oldSet = p.getSecond();
                final long oldValue = oldSet.iterator().nextLong();
                final Long oldKey = p.getFirst();
                try (ITreeCursor cursor = tm.openCursor()) {
                    if (!cursor.getSearchBoth(key(oldKey), value(oldValue))) {
                        Assert.assertTrue(cursor.getSearchBoth(key(oldKey), value(oldValue)));
                    }
                    cursor.deleteCurrent();
                }
                Assert.assertTrue(oldSet.remove(oldValue));
                if (oldSet.isEmpty()) {
                    Assert.assertEquals(oldSet, values.remove(oldKey));
                }
            }
            final long key = System.currentTimeMillis() + rnd.nextInt(count / 10);
            LongHashSet keyValues = values.get(key);
            if (keyValues == null) {
                keyValues = new LongHashSet();
                values.put(key, keyValues);
            }
            Assert.assertTrue(keyValues.add(value));
            tm.put(key(key), value(value));
        }
    }
}
