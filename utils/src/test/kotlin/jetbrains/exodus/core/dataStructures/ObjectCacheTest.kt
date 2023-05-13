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

import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.core.dataStructures.hash.HashSet
import org.junit.Assert
import org.junit.Test

class ObjectCacheTest {
    @Test
    fun cacheFiniteness() {
        val cache = ObjectCache<String, String>(4)
        cache.put("Eclipse", "An IDE")
        cache.put("IDEA", "good")
        cache.put("IDEA 4.5", "better")
        cache.put("IDEA 5.0", "perfect")
        cache.put("IDEA 6.0", "ideal")
        // "Eclipse" should already leave the cache
        Assert.assertNull(cache["Eclipse"])
    }

    @Test
    fun cacheIterator() {
        val cache = ObjectCache<String, String>(4)
        cache.put("Eclipse", "An IDE")
        cache.put("IDEA", "good IDEA")
        cache.put("IDEA 4.5", "better IDEA")
        cache.put("IDEA 5.0", "perfect IDEA")
        cache.put("IDEA 6.0", "IDEAL")
        val values = HashSet<String>()
        val it = cache.values()
        while (it.hasNext()) {
            values.add(it.next())
        }
        Assert.assertNull(cache["Eclipse"])
        Assert.assertFalse(values.contains("An IDE"))
        Assert.assertTrue(values.contains("good IDEA"))
        Assert.assertTrue(values.contains("better IDEA"))
        Assert.assertTrue(values.contains("perfect IDEA"))
        Assert.assertTrue(values.contains("IDEAL"))
    }

    private class CacheDeletedPairsListener : ObjectCache.DeletedPairsListener<String, String> {
        override fun objectRemoved(key: String, value: String) {
            removedPairs[key] = value
        }
    }

    @Test
    fun cacheListeners() {
        val cache = ObjectCache<String, String>(4)
        cache.addDeletedPairsListener(CacheDeletedPairsListener())
        removedPairs.clear()
        cache.put("Eclipse", "An IDE")
        cache.put("Eclipses", "IDEs")
        cache.put("IDEA", "good IDEA")
        cache.put("IDEA 4.5", "better IDEA")
        cache.put("IDEA 5.0", "perfect IDEA")
        cache.put("IDEA 6.0", "IDEAL")
        Assert.assertEquals("An IDE", removedPairs["Eclipse"])
        Assert.assertEquals("IDEs", removedPairs["Eclipses"])
    }

    @Test
    fun cacheListeners2() {
        val cache = ObjectCache<String, String>(4)
        cache.addDeletedPairsListener(CacheDeletedPairsListener())
        removedPairs.clear()
        cache.put("Eclipse", "An IDE")
        cache.put("Eclipses", "IDEs")
        cache.put("IDEA", "good IDEA")
        cache.put("IDEA 4.5", "better IDEA")
        cache.tryKey("Eclipse")
        cache.tryKey("Eclipses")
        Assert.assertTrue(removedPairs.isEmpty())
        Assert.assertEquals(4, cache.count().toLong())
    }

    companion object {
        private val removedPairs = HashMap<String, String>()
    }
}
