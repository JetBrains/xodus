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

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class ObjectCacheTest {

    private static final HashMap<String, String> removedPairs = new HashMap<>();

    @Test
    public void cacheFiniteness() {
        final ObjectCache<String, String> cache = new ObjectCache<>(4);
        cache.put("Eclipse", "An IDE");
        cache.put("IDEA", "good");
        cache.put("IDEA 4.5", "better");
        cache.put("IDEA 5.0", "perfect");
        cache.put("IDEA 6.0", "ideal");
        // "Eclipse" should already leave the cache
        Assert.assertNull(cache.get("Eclipse"));
    }

    @Test
    public void cacheIterator() {
        final ObjectCache<String, String> cache = new ObjectCache<>(4);
        cache.put("Eclipse", "An IDE");
        cache.put("IDEA", "good IDEA");
        cache.put("IDEA 4.5", "better IDEA");
        cache.put("IDEA 5.0", "perfect IDEA");
        cache.put("IDEA 6.0", "IDEAL");
        HashSet<String> values = new HashSet<>();
        final Iterator<String> it = cache.values();
        while (it.hasNext()) {
            values.add(it.next());
        }
        Assert.assertNull(cache.get("Eclipse"));
        Assert.assertFalse(values.contains("An IDE"));
        Assert.assertTrue(values.contains("good IDEA"));
        Assert.assertTrue(values.contains("better IDEA"));
        Assert.assertTrue(values.contains("perfect IDEA"));
        Assert.assertTrue(values.contains("IDEAL"));
    }

    private static class CacheDeletedPairsListener implements ObjectCache.DeletedPairsListener<String, String> {

        @Override
        public void objectRemoved(String key, String value) {
            removedPairs.put(key, value);
        }
    }

    @Test
    public void cacheListeners() {
        final ObjectCache<String, String> cache = new ObjectCache<>(4);
        cache.addDeletedPairsListener(new CacheDeletedPairsListener());
        removedPairs.clear();
        cache.put("Eclipse", "An IDE");
        cache.put("Eclipses", "IDEs");
        cache.put("IDEA", "good IDEA");
        cache.put("IDEA 4.5", "better IDEA");
        cache.put("IDEA 5.0", "perfect IDEA");
        cache.put("IDEA 6.0", "IDEAL");
        Assert.assertEquals("An IDE", removedPairs.get("Eclipse"));
        Assert.assertEquals("IDEs", removedPairs.get("Eclipses"));
    }

    @Test
    public void cacheListeners2() {
        final ObjectCache<String, String> cache = new ObjectCache<>(4);
        cache.addDeletedPairsListener(new CacheDeletedPairsListener());
        removedPairs.clear();
        cache.put("Eclipse", "An IDE");
        cache.put("Eclipses", "IDEs");
        cache.put("IDEA", "good IDEA");
        cache.put("IDEA 4.5", "better IDEA");
        cache.tryKey("Eclipse");
        cache.tryKey("Eclipses");
        Assert.assertTrue(removedPairs.isEmpty());
        Assert.assertEquals(4, cache.count());
    }
}
