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
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.hash.HashSet
import org.junit.Assert
import org.junit.Test

class PersistentObjectCacheTest {
    @Test
    fun cacheFiniteness() {
        val cache = PersistentObjectCache<String, String>(4)
        cache.put("Eclipse", "An IDE")
        cache.put("IDEA", "good")
        cache.put("IDEA 4.5", "better")
        Assert.assertNotNull(cache["IDEA"])
        Assert.assertNotNull(cache["IDEA 4.5"])
        // "Eclipse" should already leave the cache
        Assert.assertNull(cache["Eclipse"])
        Assert.assertNotNull(cache["IDEA 4.5"])
        Assert.assertNotNull(cache["IDEA"])
        cache.put("IDEA 5.0", "perfect")
        cache.put("IDEA 6.0", "ideal")
        Assert.assertNotNull(cache.getObject("IDEA 6.0"))
        Assert.assertNotNull(cache.getObject("IDEA 5.0"))
        Assert.assertNotNull(cache.getObject("IDEA 4.5"))
        Assert.assertNotNull(cache.getObject("IDEA"))
    }

    @Test
    fun cacheIterator() {
        val cache = PersistentObjectCache<String, String>(4)
        cache.put("Eclipse", "An IDE")
        cache.put("IDEA", "good IDEA")
        cache.put("IDEA 4.5", "better IDEA")
        Assert.assertNotNull(cache["IDEA"])
        Assert.assertNotNull(cache["IDEA 4.5"])
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

    @Test
    fun testClone() {
        val cache = PersistentObjectCache<String, String>(4)
        cache.put("IDEA", "good IDEA")
        cache.put("NetBeans", "bad IDEA")
        val copy = cache.getClone(null)
        Assert.assertNotNull(copy["IDEA"])
        copy.put("Eclipse", "An IDE")
        Assert.assertNull(cache["Eclipse"])
        Assert.assertNotNull(cache["NetBeans"])
        Assert.assertNotNull(copy["IDEA"])
        Assert.assertNotNull(copy.remove("NetBeans"))
        Assert.assertNull(copy["NetBeans"])
        Assert.assertNotNull(cache["NetBeans"])
    }
}
