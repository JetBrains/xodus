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

import java.util.Iterator;
import java.util.Map;

public class LinkedHashMapTest {

    @Test
    public void testPutGet() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 1000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i), tested.get(i));
        }
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i), tested.put(i, Integer.toString(i + 1)));
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i + 1), tested.get(i));
        }
    }

    @Test
    public void testPutGet2() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
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
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 1000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; i += 2) {
            Assert.assertEquals(Integer.toString(i), tested.remove(i));
        }
        Assert.assertEquals(500, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals((i % 2 == 0) ? null : Integer.toString(i), tested.get(i));
        }
    }

    @Test
    public void keySet() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 10000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        int i = 10000;
        for (Integer key : tested.keySet()) {
            Assert.assertEquals(--i, key.intValue());
        }
    }

    @Test
    public void keySet2() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 10000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Iterator<Integer> it = tested.keySet().iterator();
        while (it.hasNext()) {
            final int i = it.next();
            if (i % 2 == 0) {
                it.remove();
            }
        }

        Assert.assertEquals(5000, tested.size());

        it = tested.keySet().iterator();
        for (int i = 9999; i > 0; i -= 2) {
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(i, it.next().intValue());
        }
    }

    @Test
    public void lru() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<Integer, String>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > 500;
            }
        };
        for (int i = 0; i < 1000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Assert.assertEquals(500, tested.size());
        for (int i = 0; i < 500; ++i) {
            Assert.assertNull(tested.remove(i));
        }
        Assert.assertEquals(500, tested.size());
        for (int i = 500; i < 1000; ++i) {
            Assert.assertEquals(Integer.toString(i), tested.remove(i));
        }
        Assert.assertEquals(0, tested.size());
    }

    @Test
    public void lru2() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<Integer, String>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > 1000;
            }
        };
        for (int i = 0; i < 1000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Assert.assertEquals(Integer.toString(0), tested.get(0));
        for (int i = 1000; i < 1999; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Assert.assertEquals(Integer.toString(0), tested.get(0));
        tested.put(2000, Integer.toString(2000));
        Assert.assertNull(tested.get(1000));
    }

    @Test
    public void lru3() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<Integer, String>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > 1000;
            }
        };
        for (int i = 0; i < 1000; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Assert.assertEquals(Integer.toString(999), tested.remove(999));
        Assert.assertEquals(999, tested.size());
        Assert.assertEquals(Integer.toString(0), tested.get(0));
        for (int i = 1000; i < 1999; ++i) {
            tested.put(i, Integer.toString(i));
        }
        Assert.assertEquals(Integer.toString(0), tested.get(0));
        tested.put(2000, Integer.toString(2000));
        Assert.assertNull(tested.get(1000));
    }

    @Test
    public void forEachProcedure() {
        final LinkedHashMap<Integer, String> tested = new LinkedHashMap<>();
        for (int i = 0; i < 100000; ++i) {
            tested.put(i, Integer.toString(i));
        }
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
                return object > 99500;
            }
        });
        tested.forEachValue(new ObjectProcedure<String>() {
            @Override
            public boolean execute(String object) {
                ii[0]++;
                return true;
            }
        });
        Assert.assertEquals(tested.size() + 500, ii[0]);
    }
}
