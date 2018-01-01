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
package jetbrains.exodus.core.dataStructures;

import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class LongObjectCacheTest {

    private static final LongHashMap<String> removedPairs = new LongHashMap<>();

    @Test
    public void cacheFiniteness() {
        final LongObjectCache<String> cache = new LongObjectCache<>(4);
        cache.put(5, "An IDE");
        cache.put(0, "good");
        cache.put(1, "better");
        cache.put(2, "perfect");
        cache.put(3, "ideal");
        // "Eclipse" should already leave the cache
        Assert.assertNull(cache.get(5));
    }

    @Test
    public void cacheIterator() {
        LongObjectCache<String> cache = new LongObjectCache<>(4);
        cache.put(0, "An IDE");
        cache.put(1, "good IDEA");
        cache.put(2, "better IDEA");
        cache.put(3, "perfect IDEA");
        cache.put(4, "IDEAL");
        HashSet<String> values = new HashSet<>();
        final Iterator<String> it = cache.values();
        while (it.hasNext()) {
            values.add(it.next());
        }
        Assert.assertNull(cache.get(0));
        Assert.assertFalse(values.contains("An IDE"));
        Assert.assertTrue(values.contains("good IDEA"));
        Assert.assertTrue(values.contains("better IDEA"));
        Assert.assertTrue(values.contains("perfect IDEA"));
        Assert.assertTrue(values.contains("IDEAL"));
    }

    private static class CacheDeletedPairsListener implements LongObjectCache.DeletedPairsListener<String> {
        @Override
        public void objectRemoved(long key, String value) {
            removedPairs.put(key, value);
        }
    }

    @Test
    public void cacheListeners() {
        LongObjectCache<String> cache = new LongObjectCache<>(4);
        cache.addDeletedPairsListener(new CacheDeletedPairsListener());
        removedPairs.clear();
        cache.put(0, "An IDE");
        cache.put(1, "IDEs");
        cache.put(10, "good IDEA");
        cache.put(11, "better IDEA");
        cache.put(12, "perfect IDEA");
        cache.put(13, "IDEAL");
        Assert.assertEquals("An IDE", removedPairs.get(0));
        Assert.assertEquals("IDEs", removedPairs.get(1));
    }

    @Test
    public void cacheListeners2() {
        LongObjectCache<String> cache = new LongObjectCache<>(4);
        cache.addDeletedPairsListener(new CacheDeletedPairsListener());
        removedPairs.clear();
        cache.put(0, "An IDE");
        cache.put(1, "IDEs");
        cache.put(10, "good IDEA");
        cache.put(11, "better IDEA");
        cache.tryKey(0);
        cache.tryKey(1);
        Assert.assertTrue(removedPairs.isEmpty());
        Assert.assertEquals(4, cache.count());
    }
}
