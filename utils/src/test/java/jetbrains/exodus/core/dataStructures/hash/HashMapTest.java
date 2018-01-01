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
package jetbrains.exodus.core.dataStructures.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class HashMapTest {

    @Test
    public void testPutGet() {
        final Map<Integer, String> tested = new HashMap<>();
        for (int i = 0; i < 1000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        tested.put(null, "null");
        Assert.assertEquals(1001, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i), tested.get(i));
        }
        Assert.assertEquals("null", tested.get(null));
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i), tested.put(i, Integer.toString(i + 1)));
        }
        Assert.assertEquals("null", tested.put(null, "new null"));
        Assert.assertEquals(1001, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i + 1), tested.get(i));
        }
        Assert.assertEquals("new null", tested.get(null));
    }

    @Test
    public void testPutGet2() {
        final Map<Integer, String> tested = new HashMap<>();
        for (int i = 0; i < 1000; ++i) {
            tested.put(i - 500, Integer.toString(i));
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i), tested.get(i - 500));
        }
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i), tested.put(i - 500, Integer.toString(i + 1)));
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i + 1), tested.get(i - 500));
        }
    }

    @Test
    public void testPutGetRemove() {
        final Map<Integer, String> tested = new HashMap<>();
        for (int i = 0; i < 1000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        tested.put(null, "null");
        Assert.assertEquals(1001, tested.size());
        for (int i = 0; i < 1000; i += 2) {
            Assert.assertEquals(Integer.toString(i), tested.remove(i));
        }
        Assert.assertEquals(501, tested.size());
        Assert.assertEquals("null", tested.get(null));
        tested.remove(null);
        Assert.assertEquals(null, tested.get(null));
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals((i % 2 == 0) ? null : Integer.toString(i), tested.get(i));
        }
    }

    @Test
    public void keySet() {
        final Map<Integer, String> tested = new HashMap<>();
        final Set<Integer> set = new HashSet<>();

        for (int i = 0; i < 10000; ++i) {
            tested.put(i, Integer.toString(i));
            set.add(i);
        }
        tested.put(null, "null");
        set.add(null);
        for (Integer key : tested.keySet()) {
            Assert.assertTrue(set.remove(key));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void keySet2() {
        final Map<Integer, String> tested = new HashMap<>();
        final Set<Integer> set = new HashSet<>();

        for (int i = 0; i < 10000; ++i) {
            tested.put(i, Integer.toString(i));
            set.add(i);
        }
        tested.put(null, "null");
        set.add(null);
        Iterator<Integer> it = tested.keySet().iterator();
        while (it.hasNext()) {
            final Integer i = it.next();
            it.remove();
            Assert.assertTrue(set.remove(i));
            if (it.hasNext()) {
                it.next();
            }
        }

        Assert.assertEquals(5000, tested.size());

        it = tested.keySet().iterator();
        for (int i = 9998; i >= 0; i -= 2) {
            Assert.assertTrue(it.hasNext());
            Assert.assertTrue(it.next() % 2 == 0);
            Assert.assertTrue(set.remove(i));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testCopy() {
        final HashMap<Integer, String> tested = new HashMap<>();
        tested.put(7, "a");
        tested.put(8, "b");
        HashMap<Integer, String> copy = new HashMap<>(tested);
        Assert.assertEquals("a", copy.get(7));
        Assert.assertEquals("b", copy.get(8));
        Assert.assertEquals(2, copy.size);
    }

    @Test
    public void testCopyAndModify() {
        final HashMap<Integer, String> tested = new HashMap<>();
        tested.put(7, "a");
        tested.put(8, "b");
        HashMap<Integer, String> copy = new HashMap<>(tested);
        tested.put(7, "c");
        Assert.assertEquals("a", copy.get(7));
        Assert.assertEquals("b", copy.get(8));
        Assert.assertEquals(2, copy.size);
    }

    @Test
    public void forEachProcedure() {
        final HashMap<Integer, String> tested = new HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        tested.put(null, "null");
        final int[] ii = {0};
        tested.forEachKey(new ObjectProcedure<Integer>() {
            @Override
            public boolean execute(Integer object) {
                ii[0]++;
                return true;
            }
        });
        tested.forEachValue(new ObjectProcedure<String>() {
            @Override
            public boolean execute(String object) {
                ii[0]++;
                return true;
            }
        });
        Assert.assertEquals(tested.size() * 2, ii[0]);
        ii[0] = 0;
        tested.forEachKey(new ObjectProcedure<Integer>() {
            @Override
            public boolean execute(Integer object) {
                ii[0]++;
                return (object == null) || (object < 500);
            }
        });
        tested.forEachValue(new ObjectProcedure<String>() {
            @Override
            public boolean execute(String object) {
                ii[0]++;
                return true;
            }
        });
        Assert.assertEquals(tested.size() + 502, ii[0]);
    }
}
