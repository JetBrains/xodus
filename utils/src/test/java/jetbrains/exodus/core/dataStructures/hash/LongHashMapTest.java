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
import java.util.Set;

public class LongHashMapTest {

    @Test
    public void testPutGet() {
        final Map<Long, String> tested = new LongHashMap<>();
        for (long i = 0; i < 1000; ++i) {
            tested.put(i, Long.toString(i));
        }
        Assert.assertEquals(1000, tested.size());
        for (long i = 0; i < 1000; ++i) {
            Assert.assertEquals(Long.toString(i), tested.get(i));
        }
        for (long i = 0; i < 1000; ++i) {
            Assert.assertEquals(Long.toString(i), tested.put(i, Long.toString(i + 1)));
        }
        Assert.assertEquals(1000, tested.size());
        for (long i = 0; i < 1000; ++i) {
            Assert.assertEquals(Long.toString(i + 1), tested.get(i));
        }
    }

    @Test
    public void testPutGet2() {
        final Map<Long, String> tested = new LongHashMap<>();
        for (long i = 0; i < 1000; ++i) {
            tested.put(i - 500, Long.toString(i));
        }
        Assert.assertEquals(1000, tested.size());
        for (long i = 0; i < 1000; ++i) {
            Assert.assertEquals(Long.toString(i), tested.get(i - 500));
        }
        for (long i = 0; i < 1000; ++i) {
            Assert.assertEquals(Long.toString(i), tested.put(i - 500, Long.toString(i + 1)));
        }
        Assert.assertEquals(1000, tested.size());
        for (long i = 0; i < 1000; ++i) {
            Assert.assertEquals(Long.toString(i + 1), tested.get(i - 500));
        }
    }

    @Test
    public void testPutGetRemove() {
        final Map<Long, String> tested = new LongHashMap<>();
        for (long i = 0; i < 1000; ++i) {
            tested.put(i, Long.toString(i));
        }
        Assert.assertEquals(1000, tested.size());
        for (long i = 0; i < 1000; i += 2) {
            Assert.assertEquals(Long.toString(i), tested.remove(i));
        }
        Assert.assertEquals(500, tested.size());
        for (long i = 0; i < 1000; ++i) {
            Assert.assertEquals((i % 2 == 0) ? null : Long.toString(i), tested.get(i));
        }
    }

    @Test
    public void keySet() {
        final Map<Long, String> tested = new LongHashMap<>();
        final Set<Long> set = new LongHashSet();

        for (long i = 0; i < 10000; ++i) {
            tested.put(i, Long.toString(i));
            set.add(i);
        }
        for (Long key : tested.keySet()) {
            Assert.assertTrue(set.remove(key));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void keySet2() {
        final Map<Long, String> tested = new LongHashMap<>();
        final Set<Long> set = new LongHashSet();

        for (long i = 0; i < 10000; ++i) {
            tested.put(i, Long.toString(i));
            set.add(i);
        }
        Iterator<Long> it = tested.keySet().iterator();
        while (it.hasNext()) {
            final long i = it.next();
            if (i % 2 == 0) {
                it.remove();
                Assert.assertTrue(set.remove(i));
            }
        }

        Assert.assertEquals(5000, tested.size());

        it = tested.keySet().iterator();
        for (long i = 9999; i > 0; i -= 2) {
            Assert.assertTrue(it.hasNext());
            Assert.assertTrue(it.next() % 2 != 0);
            Assert.assertTrue(set.remove(i));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void forEachProcedure() {
        final IntHashMap<String> tested = new IntHashMap<>();
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
                return object < 500;
            }
        });
        tested.forEachValue(new ObjectProcedure<String>() {
            @Override
            public boolean execute(String object) {
                ii[0]++;
                return true;
            }
        });
        Assert.assertEquals(tested.size() + 501, ii[0]);
    }
}
