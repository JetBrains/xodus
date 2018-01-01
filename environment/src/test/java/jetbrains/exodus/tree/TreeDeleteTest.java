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
package jetbrains.exodus.tree;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class TreeDeleteTest extends TreeBaseTest {

    @Test
    public void testDeleteNoDuplicates() throws IOException {
        tm = createMutableTree(false, 1);

        tm.put(kv(1, "1"));
        assertEquals(1, tm.getSize());
        assertEquals(value("1"), tm.get(key(1)));
        assertEquals(false, tm.delete(key(2)));
        assertEquals(true, tm.delete(key(1)));
        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));

        long a = tm.save();
        reopen();
        t = openTree(a, false);

        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));
    }

    @Test
    public void testDeleteNoDuplicates2() throws IOException {
        tm = createMutableTree(false, 1);

        tm.put(key("1"), value("1"));
        tm.put(key("11"), value("11"));
        tm.put(key("111"), value("111"));

        long a = tm.save();
        reopen();
        t = openTree(a, false);

        tm = t.getMutableCopy();

        assertEquals(3, tm.getSize());
        assertEquals(value("1"), tm.get(key("1")));
        assertEquals(value("11"), tm.get(key("11")));
        assertEquals(value("111"), tm.get(key("111")));
        assertEquals(false, tm.delete(key(2)));
        assertEquals(true, tm.delete(key("111")));
        assertEquals(true, tm.delete(key("11")));
        assertEquals(1, tm.getSize());
        assertEquals(null, tm.get(key("11")));

        a = tm.save();
        reopen();
        t = openTree(a, false);

        assertEquals(1, tm.getSize());
        assertEquals(null, tm.get(key("111")));
        assertEquals(null, tm.get(key("11")));
        valueEquals("1", tm.get(key("1")));
    }

    /*@Test
    public void testDeleteNoDuplicates2() throws IOException {
        tm = createMutableTree(false);

        getTreeMutable().put(kv(1, "1"));
        getTreeMutable().put(kv(2, "2"));
        getTreeMutable().put(kv(3, "3"));
        getTreeMutable().put(kv(4, "4"));
        getTreeMutable().put(kv(5, "5"));

        assertEquals(5, tm.getSize());

        long a = tm.save();
        tm = openTree(a, false).getMutableCopy();

        System.out.println("Orig-----------------------");
        dump(getTreeMutable());
        assertEquals(5, tm.getSize());

        tm.delete(key(1));
        tm.delete(key(2));
        tm.delete(key(3));

        assertEquals(2, tm.getSize());
        System.out.println("After delete-----------------------");
        dump(getTreeMutable());
        writeItems(tm);

        a = tm.save();

        assertEquals(2, tm.getSize());
        System.out.println("After delete and save-----------------------");
        dump(getTreeMutable());
        writeItems(tm);

        tm = openTree(a, false).getMutableCopy();
        assertEquals(2, tm.getSize());
        System.out.println("After delete, save and reopen-----------------------");
        dump(getTreeMutable());
        writeItems(tm);

        tm.delete(key(4));
        System.out.println("After delete 4-----------------------");
        dump(getTreeMutable());
        writeItems(tm);

        tm.delete(key(5));
        System.out.println("After delete 5-----------------------");
        dump(getTreeMutable());
        writeItems(tm);

        assertEquals(0, tm.getSize());
        a = tm.save();
        t = openTree(a, false);

        assertTrue(t.isEmpty());
        assertEquals(0, t.getSize());
        assertFalse(t.openCursor().getNext());
    } */

    @Test
    public void testDeleteNotExistingKey() throws IOException {
        tm = createMutableTree(false, 1);

        getTreeMutable().put(kv(1, "1"));
        assertEquals(1, tm.getSize());
        assertEquals(value("1"), tm.get(key(1)));
        assertEquals(false, tm.delete(key(-1)));
        assertEquals(false, tm.delete(key(-2)));
        assertEquals(false, tm.delete(key(-3)));
        assertEquals(false, tm.delete(key(2)));
        assertEquals(true, tm.delete(key(1)));
        assertEquals(false, tm.delete(key(1)));
        assertEquals(false, tm.delete(key(-1)));
        assertEquals(false, tm.delete(key(-2)));

        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));

        long a = tm.save();
        reopen();
        t = openTree(a, false);

        assertEquals(0, t.getSize());
        assertEquals(null, t.get(key(1)));
        assertEquals(null, t.get(key(-1)));
        assertEquals(null, t.get(key(2)));
    }

    @Test
    public void testDeleteNotExistingKey2() throws IOException {
        tm = createMutableTree(false, 1);

        getTreeMutable().put(kv(1, "1"));
        getTreeMutable().put(kv(11, "1"));
        getTreeMutable().put(kv(111, "1"));
        assertEquals(3, tm.getSize());
        assertEquals(value("1"), tm.get(key(1)));
        assertEquals(false, tm.delete(key(-1)));
        assertEquals(false, tm.delete(key(-2)));
        assertEquals(false, tm.delete(key(-3)));
        assertEquals(false, tm.delete(key(2)));
        assertEquals(true, tm.delete(key(1)));
        assertEquals(false, tm.delete(key(1)));
        assertEquals(true, tm.delete(key(11)));
        assertEquals(false, tm.delete(key(11)));
        assertEquals(true, tm.delete(key(111)));
        assertEquals(false, tm.delete(key(111)));
        assertEquals(false, tm.delete(key(-1)));
        assertEquals(false, tm.delete(key(-2)));

        assertEquals(0, tm.getSize());
        assertEquals(null, tm.get(key(1)));

        long a = tm.save();
        reopen();
        t = openTree(a, false);

        assertEquals(0, t.getSize());
        assertEquals(null, t.get(key(1)));
        assertEquals(null, t.get(key(-1)));
        assertEquals(null, t.get(key(2)));
    }

    @Test
    public void testPutDeleteRandomWithoutDuplicates() throws Throwable {
        tm = createMutableTree(false, 1);

        final IntHashMap<String> map = new IntHashMap<>();
        final int count = 30000;

        TestUtil.time("Put took ", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < count; ++i) {
                    final int key = Math.abs(RANDOM.nextInt());
                    final String value = Integer.toString(i);
                    tm.put(key(Integer.toString(key)), value(value));
                    map.put(key, value);
                }
            }
        });

        long address = tm.save();
        reopen();
        t = openTree(address, false);

        tm = t.getMutableCopy();
        TestUtil.time("Delete took ", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < count; ++i) {
                    final int key = Math.abs(RANDOM.nextInt());
                    assertEquals(map.remove(key) != null, tm.delete(key(Integer.toString(key))));
                }
            }
        });

        address = tm.save();
        reopen();
        t = openTree(address, false);

        assertEquals(map.size(), t.getSize());

        TestUtil.time("Get took ", new Runnable() {
            @Override
            public void run() {
                for (final Map.Entry<Integer, String> entry : map.entrySet()) {
                    final Integer key = entry.getKey();
                    final String value = entry.getValue();
                    valueEquals(value, t.get(key(Integer.toString(key))));
                }
            }
        });

        tm = t.getMutableCopy();

        TestUtil.time("Missing get took ", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < count; ++i) {
                    final int key = Math.abs(RANDOM.nextInt());
                    if (!map.containsKey(key)) {
                        assertEquals(false, tm.delete(key(Integer.toString(key))));
                    }
                }
            }
        });
    }
}
