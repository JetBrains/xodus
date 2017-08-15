/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.TestFor;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

public abstract class TreeCursorNoDuplicatesTest extends CursorTestBase {

    @Before
    public void prepareTree() {
        tm = createMutableTree(false, 1).getMutableCopy();

        for (int i = 0; i < s; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }
    }

    @Test
    public void testOneNode() throws IOException {
        tm = createMutableTree(false, 1);
        getTreeMutable().put(kv(1, "v1"));
        assertEquals(1, getTreeMutable().getSize());
        ITreeCursor cursor = getTreeMutable().openCursor();
        assertTrue(cursor.getNext());
        assertFalse(cursor.getNext());
    }

    @Test
    public void testGetNext() throws IOException {
        final GetNext getNext = new GetNext() {
            @Override
            public boolean n(Cursor c) {
                return c.getNext();
            }
        };
        check(tm, getNext);
        long a = getTreeMutable().save();
        check(tm, getNext);
        reopen();
        t = openTree(a, false);
        check(t, getNext);
    }

    @Test
    public void testGetNext2() throws IOException {
        tm = createMutableTree(false, 1);

        getTreeMutable().put(kv(1, "v1"));
        getTreeMutable().put(kv(2, "v2"));
        getTreeMutable().put(kv(3, "v3"));
        getTreeMutable().put(kv(4, "v4"));
        getTreeMutable().put(kv(5, "v5"));

        Cursor c = getTreeMutable().openCursor();
        assertEquals(value("v5"), c.getSearchKey(key(5)));
        assertFalse(c.getNext());

        long a = getTreeMutable().save();
        c = getTreeMutable().openCursor();
        assertEquals(value("v5"), c.getSearchKey(key(5)));
        assertFalse(c.getNext());

        t = openTree(a, false);
        c = getTreeMutable().openCursor();
        assertEquals(value("v5"), c.getSearchKey(key(5)));
        assertFalse(c.getNext());
    }

    @Test
    public void testGetNext3() throws IOException {
        tm = createMutableTree(false, 1);

        for (int i = 0; i < 1000; i++) {
            getTreeMutable().put(kv(i, "v" + i));
        }

        Cursor c = getTreeMutable().openCursor();
        assertEquals(value("v998"), c.getSearchKey(key(998)));
        assertTrue(c.getNext()); //v999 - last
        assertFalse(c.getNext());
    }

    @Test
    public void testCount() throws IOException {
        final GetNext getNext = new GetNext() {
            @Override
            public boolean n(Cursor c) {
                return c.getNext() && c.count() == 1;
            }
        };
        check(tm, getNext);
        long a = getTreeMutable().save();
        check(tm, getNext);
        reopen();
        t = openTree(a, true);
        check(tm, getNext);
    }

    @Test
    public void testGetNextNoDup() throws IOException {
        final GetNext getNextNoDup = new GetNext() {
            @Override
            public boolean n(Cursor c) {
                return c.getNextNoDup();
            }
        };
        check(tm, getNextNoDup);
        long a = getTreeMutable().save();
        check(tm, getNextNoDup);
        reopen();
        t = openTree(a, true);
        check(tm, getNextNoDup);
    }

    @Test
    public void testGetSearchKey() throws IOException {
        Cursor c = getTreeMutable().openCursor();

        for (int i = 0; i < s; i++) {
            assertEquals(value("v" + i), c.getSearchKey(key(i)));
            assertEquals(c.getValue(), value("v" + i));
            assertEquals(c.getKey(), key(i));
        }

        assertFalse(c.getNext());
    }

    @Test
    public void testGetSearchBoth() throws IOException {
        Cursor c = getTreeMutable().openCursor();

        for (int i = 0; i < s; i++) {
            assertEquals(true, c.getSearchBoth(key(i), value("v" + i)));
            assertEquals(c.getValue(), value("v" + i));
            assertEquals(c.getKey(), key(i));
        }

        assertFalse(c.getNext());
    }

    @Test
    public void testGetSearchBoth2() throws IOException {
        tm = createMutableTree(false, 1);
        tm.put(kv("1", "2"));
        final long address = tm.save();
        final ITreeCursor cursor = openTree(address, false).openCursor();
        assertFalse(cursor.getSearchBoth(key("1"), value("1")));
    }

    @Test
    public void testGetSearchKeyRange1() throws IOException {
        Cursor c = getTreeMutable().openCursor();

        for (int i = 0; i < s; i++) {
            assertEquals(value("v" + i), c.getSearchKeyRange(key(i)));
            assertEquals(c.getValue(), value("v" + i));
            assertEquals(c.getKey(), key(i));
        }

        assertFalse(c.getNext());
    }

    @Test
    public void testGetSearchKeyRange2() throws IOException {
        tm = createMutableTree(false, 1);

        getTreeMutable().put(key("10"), value("v10"));
        getTreeMutable().put(key("20"), value("v20"));
        getTreeMutable().put(key("30"), value("v30"));
        getTreeMutable().put(key("40"), value("v40"));
        getTreeMutable().put(key("50"), value("v50"));
        getTreeMutable().put(key("60"), value("v60"));

        Cursor c = getTreeMutable().openCursor();
        assertEquals(value("v10"), c.getSearchKeyRange(key("01")));
        assertEquals(key("10"), c.getKey());

        assertEquals(value("v60"), c.getSearchKeyRange(key("55")));
        assertEquals(key("60"), c.getKey());

        assertEquals(null, c.getSearchKeyRange(key("61")));
        // cursor keep prev pos
        assertEquals(key("60"), c.getKey());

        assertFalse(c.getNext());
    }

    @Test
    public void testGetSearchKeyRange3() throws IOException {
        tm = createMutableTree(false, 1);

        getTreeMutable().put(new ArrayByteIterable(new byte[]{1}), value("v1"));
        final ArrayByteIterable key = new ArrayByteIterable(new byte[]{1, 2, 1, 0});
        getTreeMutable().put(key, value("v2"));

        Cursor c = getTreeMutable().openCursor();
        assertEquals(value("v2"), c.getSearchKeyRange(new ArrayByteIterable(new byte[]{1, 2, 1})));
        assertEquals(key, c.getKey());

        assertFalse(c.getNext());
    }

    @Test
    public void testGetSearchKeyRange4() throws IOException {
        tm = createMutableTree(false, 1);

        final ByteIterable v = value("0");
        tm.put(key("aaaa"), v);
        tm.put(key("aaab"), v);
        tm.put(key("aaba"), v);

        Cursor c = tm.openCursor();

        c.getSearchKeyRange(key("aaac"));
        assertEquals(key("aaba"), c.getKey());
    }

    @Test
    public void testGetSearchKeyRange5() throws IOException {
        tm = createMutableTree(false, 1);

        final ByteIterable v = value("0");
        tm.put(key("aaba"), v);
        tm.put(key("aabb"), v);

        Cursor c = tm.openCursor();

        assertNotNull(c.getSearchKeyRange(key("aababa")));
        assertEquals(key("aabb"), c.getKey());
    }

    @Test
    public void testGetSearchBothRange1() throws IOException {
        Cursor c = getTreeMutable().openCursor();

        for (int i = 0; i < s; i++) {
            assertEquals(value("v" + i), c.getSearchBothRange(key(i), value("v" + i)));
            assertEquals(c.getValue(), value("v" + i));
            assertEquals(c.getKey(), key(i));
        }
    }

    @Test
    public void testGetSearchBothRange2() throws IOException {
        tm = getTreeMutable().getMutableCopy();

        getTreeMutable().put(key("10"), value("v10"));
        getTreeMutable().put(key("20"), value("v20"));
        getTreeMutable().put(key("30"), value("v30"));
        getTreeMutable().put(key("40"), value("v40"));
        getTreeMutable().put(key("50"), value("v50"));
        getTreeMutable().put(key("60"), value("v60"));

        Cursor c = getTreeMutable().openCursor();
        // miss
        assertEquals(null, c.getSearchBothRange(key("01"), value("v10")));

        // found
        assertEquals(value("v10"), c.getSearchBothRange(key("10"), value("v01")));

        // miss
        assertEquals(null, c.getSearchBothRange(key("20"), value("v21")));

        // check keep prev state
        assertEquals(key("10"), c.getKey());
    }

    @Test
    public void testGetPrev() throws IOException {
        final GetPrev getPrev = new GetPrev() {
            @Override
            public boolean p(Cursor c) {
                return c.getPrev();
            }
        };
        long a = getTreeMutable().save();
        reopen();
        t = openTree(a, false);
        check(t, getPrev);
    }

    @Test
    public void testGetPrev2() throws IOException {
        tm = createMutableTree(true, 1);
        tm.put(kv("the", "fuck"));
        ITreeCursor c = tm.openCursor();
        assertTrue(c.getPrev());
        c.close();
    }

    @Test
    public void testGetLast_XD_466() throws IOException {
        tm = createMutableTree(true, 1);
        try (ITreeCursor c = tm.openCursor()) {
            assertFalse(c.getLast());
        }
        for (int i = 0; i < 9999; ++i) {
            final StringKVNode kv = (StringKVNode) kv(i, Integer.toString(i));
            tm.put(kv);
            try (ITreeCursor c = tm.openCursor()) {
                assertTrue(c.getLast());
                assertEquals(kv.getKey(), c.getKey());
                assertEquals(kv.getValue(), c.getValue());
                if (i > 0) {
                    assertTrue(c.getPrev());
                    assertNotEquals(kv.getKey(), c.getKey());
                    assertNotEquals(kv.getValue(), c.getValue());
                }
                assertTrue(c.getLast());
                assertEquals(kv.getKey(), c.getKey());
                assertEquals(kv.getValue(), c.getValue());
            }
        }
    }

    @Test
    public void testSplitRange() throws IOException {
        tm = getTreeMutable().getMutableCopy();

        getTreeMutable().put(key("aaabbb"), value("v10"));
        getTreeMutable().put(key("aaaddd"), value("v20"));

        Cursor c = getTreeMutable().openCursor();

        assertNotNull(c.getSearchKeyRange(key("aaa")));

        assertEquals(value("v10"), c.getValue());

        assertNull(c.getSearchKey(key("aaa")));
    }

    @Test
    public void testSplitRange2() throws IOException {
        tm = createMutableTree(false, 1);
        getTreeMutable().put(key("aa"), value("v"));
        getTreeMutable().put(key("ab"), value("v"));
        ITreeCursor cursor = getTreeMutable().openCursor();
        assertNull(cursor.getSearchKeyRange(key("bb")));
    }

    @Test
    public void xd_333() throws IOException {
        rnd = new Random(0);
        final ByteIterable value = value("value");
        tm = createMutableTree(false, 1);
        final TreeSet<String> keys = new TreeSet<>();
        for (int i = 0; i < 15; ++i) {
            final String key = rndString();
            tm.put(key(key), value);
            keys.add(key);
        }
        /*final long address = tm.save();
        reopen();
        final ITree t = openTree(address, false);*/
        testCursorOrder(keys);
    }

    @Test
    public void testOrderedInserts() {
        final ByteIterable value = value("value");
        final TreeSet<String> keys = new TreeSet<>();
        for (int i = 0; i < 10000; ++i) {
            keys.add(rndString());
        }
        tm = createMutableTree(false, 1);
        for (final String key : keys) {
            Assert.assertTrue(tm.add(key(key), value));
        }
        testCursorOrder(keys);
        tm = createMutableTree(false, 1);
        for (final String key : keys.descendingSet()) {
            Assert.assertTrue(tm.add(key(key), value));
        }
        testCursorOrder(keys);
    }

    @Test
    public void testRandomInserts() {
        final ByteIterable value = value("value");
        final Set<String> keys = new HashSet<>();
        for (int i = 0; i < 10000; ++i) {
            keys.add(rndString());
        }
        tm = createMutableTree(false, 1);
        for (final String key : keys) {
            Assert.assertTrue(tm.add(key(key), value));
        }
        testCursorOrder(new TreeSet<>(keys));
    }

    @Test
    public void testInsertDeletes() {
        final ByteIterable value = value("value");
        final TreeSet<String> keys = new TreeSet<>();
        tm = createMutableTree(false, 1);
        for (int i = 0; i < 10000; ++i) {
            final String key = rndString();
            if (keys.add(key)) {
                Assert.assertTrue(tm.add(key(key), value));
            }
            if (keys.size() > 1000) {
                final String obsoleteKey = keys.first();
                keys.remove(obsoleteKey);
                Assert.assertTrue(tm.delete(key(obsoleteKey)));
            }
        }
        testCursorOrder(keys);
    }

    @Test
    public void testInsertDeletes2() {
        final ByteIterable value = value("value");
        final Set<String> keys = new HashSet<>();
        tm = createMutableTree(false, 1);
        for (int i = 0; i < 10000; ++i) {
            final String key = rndString();
            if (keys.add(key)) {
                Assert.assertTrue(tm.add(key(key), value));
            }
            if (keys.size() > 1000) {
                final String obsoleteKey = keys.iterator().next();
                keys.remove(obsoleteKey);
                Assert.assertTrue(tm.delete(key(obsoleteKey)));
            }
        }
        testCursorOrder(new TreeSet<>(keys));
    }

    @Test
    @TestFor(issues = "XD-614")
    public void failingGetNextAndGetPrevDontInvalidateKeyValue() {
        tm = createMutableTree(false, 1);
        tm.put(kv("0", "0"));
        try (Cursor cursor = tm.openCursor()) {
            Assert.assertTrue(cursor.getNext());
            Assert.assertEquals(key("0"), cursor.getKey());
            Assert.assertEquals(key("0"), cursor.getValue());
            Assert.assertFalse(cursor.getNext());
            Assert.assertEquals(key("0"), cursor.getKey());
            Assert.assertEquals(key("0"), cursor.getValue());
        }
        try (Cursor cursor = tm.openCursor()) {
            Assert.assertTrue(cursor.getPrev());
            Assert.assertEquals(key("0"), cursor.getKey());
            Assert.assertEquals(key("0"), cursor.getValue());
            Assert.assertFalse(cursor.getPrev());
            Assert.assertEquals(key("0"), cursor.getKey());
            Assert.assertEquals(key("0"), cursor.getValue());
        }
    }

    @Test
    @TestFor(issues = "XD-619")
    public void failingGetNextAndGetPrevDontInvalidateKeyValue2() {
        tm = createMutableTree(false, 1);
        final int treeSize = 10000;
        for (int i = 0; i < treeSize; ++i) {
            tm.put(kv(i, Integer.toString(i)));
        }
        try (Cursor cursor = tm.openCursor()) {
            ByteIterable key = null;
            while (cursor.getNext()) {
                key = cursor.getKey();
            }
            Assert.assertEquals(key(treeSize - 1), key);
            Assert.assertEquals(key(treeSize - 1), cursor.getKey());
            cursor.getNext();
            cursor.getNext();
            Assert.assertEquals(key(treeSize - 1), cursor.getKey());
            cursor.getPrev();
            Assert.assertEquals(key(treeSize - 2), cursor.getKey());
        }
        try (Cursor cursor = tm.openCursor()) {
            ByteIterable key = null;
            while (cursor.getPrev()) {
                key = cursor.getKey();
            }
            Assert.assertEquals(key(0), key);
            Assert.assertEquals(key(0), cursor.getKey());
            cursor.getPrev();
            cursor.getPrev();
            Assert.assertEquals(key(0), cursor.getKey());
            cursor.getNext();
            Assert.assertEquals(key(1), cursor.getKey());
        }
    }

    private void testCursorOrder(final TreeSet<String> keys) {
        final ITreeCursor cursor = tm.openCursor();
        for (final String key : keys) {
            Assert.assertNotNull(tm.get(key(key)));
            Assert.assertTrue(cursor.getNext());
            valueEquals(key, cursor.getKey());
        }
        cursor.close();
    }

    private static Random rnd = new Random();

    private static String rndString() {
        final int len = rnd.nextInt(4) + 1;
        final StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            builder.append((char) ('0' + rnd.nextInt(10)));
        }
        return builder.toString();
    }
}
