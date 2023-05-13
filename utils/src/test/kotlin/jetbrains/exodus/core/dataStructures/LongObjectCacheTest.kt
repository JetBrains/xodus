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
package jetbrains.exodus.core.dataStructures

import jetbrains.exodus.core.dataStructures.hash.HashSet
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import org.junit.Assert
import org.junit.Test

class LongObjectCacheTest {
    @Test
    fun cacheFiniteness() {
        val cache = LongObjectCache<String>(4)
        cache.put(5, "An IDE")
        cache.put(0, "good")
        cache.put(1, "better")
        cache.put(2, "perfect")
        cache.put(3, "ideal")
        // "Eclipse" should already leave the cache
        Assert.assertNull(cache[5])
    }

    @Test
    fun cacheIterator() {
        val cache = LongObjectCache<String>(4)
        cache.put(0, "An IDE")
        cache.put(1, "good IDEA")
        cache.put(2, "better IDEA")
        cache.put(3, "perfect IDEA")
        cache.put(4, "IDEAL")
        val values = HashSet<String>()
        val it = cache.values()
        while (it.hasNext()) {
            values.add(it.next())
        }
        Assert.assertNull(cache[0])
        Assert.assertFalse(values.contains("An IDE"))
        Assert.assertTrue(values.contains("good IDEA"))
        Assert.assertTrue(values.contains("better IDEA"))
        Assert.assertTrue(values.contains("perfect IDEA"))
        Assert.assertTrue(values.contains("IDEAL"))
    }

    private class CacheDeletedPairsListener : LongObjectCache.DeletedPairsListener<String> {
        override fun objectRemoved(key: Long, value: String) {
            removedPairs[key] = value
        }
    }

    @Test
    fun cacheListeners() {
        val cache = LongObjectCache<String>(4)
        cache.addDeletedPairsListener(CacheDeletedPairsListener())
        removedPairs.clear()
        cache.put(0, "An IDE")
        cache.put(1, "IDEs")
        cache.put(10, "good IDEA")
        cache.put(11, "better IDEA")
        cache.put(12, "perfect IDEA")
        cache.put(13, "IDEAL")
        Assert.assertEquals("An IDE", removedPairs[0])
        Assert.assertEquals("IDEs", removedPairs[1])
    }

    @Test
    fun cacheListeners2() {
        val cache = LongObjectCache<String>(4)
        cache.addDeletedPairsListener(CacheDeletedPairsListener())
        removedPairs.clear()
        cache.put(0, "An IDE")
        cache.put(1, "IDEs")
        cache.put(10, "good IDEA")
        cache.put(11, "better IDEA")
        cache.tryKey(0)
        cache.tryKey(1)
        Assert.assertTrue(removedPairs.isEmpty())
        Assert.assertEquals(4, cache.count().toLong())
    }

    companion object {
        private val removedPairs = LongHashMap<String>()
    }
}
