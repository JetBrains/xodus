/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.core.cache.persistent

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.time.Duration

class CaffeinePersistentCacheTest {

    @Test
    fun `should put and then get with the same key`() {
        // Given
        val cache = givenSizedCache(2)

        // When
        cache.put("key", "value1")
        cache.put("key", "value2")

        // Then
        assertEquals("value2", cache.get("key"))
    }

    @Test
    fun `should put and then get with different keys`() {
        // Given
        val cache = givenSizedCache(2)

        // When
        cache.put("key1", "value1")
        cache.put("key2", "value2")

        // Then
        assertEquals("value1", cache.get("key1"))
        assertEquals("value2", cache.get("key2"))
    }

    @Test
    fun `should remove entry`() {
        // Given
        val cache = givenSizedCache(2)
        cache.put("key1", "value1")
        cache.put("key2", "value2")

        // When
        cache.remove("key1")

        // Then
        assertEquals(null, cache.get("key1"))
        assertEquals("value2", cache.get("key2"))
        assertEquals(1, cache.count())
    }

    @Test
    fun `should evict`() {
        // Given
        val cache = givenSizedCache(1)

        // When
        cache.put("key1", "value1")
        cache.put("key2", "value2")

        // Then
        assertEquals(null, cache.get("key1"))
        assertEquals("value2", cache.get("key2"))
    }

    @Test
    fun `should be versioned when put`() {
        // Given
        val cache1 = givenSizedCache(2)
        // Register client to prevent cache from being removed for it's version
        cache1.registerClient()
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key", "value1")
        cache2.put("key", "value2")

        // Then
        assertEquals("value1", cache1.get("key"))
        assertEquals("value2", cache2.get("key"))
    }

    @Test
    fun `should be versioned when remove`() {
        // Given
        val cache1 = givenSizedCache(10)
        val cache2 = cache1.createNextVersion()
        val cache3 = cache2.createNextVersion()

        // When
        cache1.put("key", "value")
        // Remove should not affect previous versions
        cache2.remove("key")

        // Then
        assertEquals("value", cache1.get("key"))
        assertNull(cache2.get("key"))
        assertNull(cache3.get("key"))
    }

    @Test
    fun `should evict versioned based on size`() {
        // Given
        val cache1 = givenSizedCache(1)
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key1", "value1")
        cache2.put("key2", "value2")

        // Then
        assertEquals(1, cache2.count())
        assertEquals("value2", cache2.get("key2"))
    }

    @Test
    fun `should evict based on time`() {
        // Given
        val cache1 = givenTimedCache(Duration.ofMillis(1))
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key1", "value1")
        cache2.put("key2", "value2")
        Thread.sleep(2)
        cache1.forceEviction()

        // Then
        assertEquals(0, cache2.count())
        assertNull(cache1.get("key1"))
        assertNull(cache2.get("key2"))
    }


    @Test
    fun `should remove old version when client unregisters`() {
        // Given
        val cache1 = givenSizedCache(2)
        val cache2 = cache1.createNextVersion()

        // When register
        val registration = cache1.registerClient()
        cache1.put("key", "value1")
        cache2.put("key", "value2")
        assertEquals("value1", cache1.get("key"))

        // When unregister
        registration.unregister()

        // Then
        assertEquals("value2", cache2.get("key")) // also triggers eviction of old value
        assertEquals(null, cache1.get("key"))
    }

    @Test
    fun `should not evict same values for next version`() {
        // Given
        val n = 100
        val cache1 = givenSizedCache(n.toLong())
        // Fill in cache up to capacity
        repeat(n) { cache1.put("$it", "$it") }

        // When
        val cache2 = cache1.createNextVersion()

        // Then
        assertEquals(100, cache2.count())
        repeat(n) { assertEquals("$it", cache2.get("$it")) }
    }

    @Test
    fun `should keep local index in sync with cache`() {
        // Given
        val cache = givenSizedCache(1)

        // When-Then
        cache.put("key1", "value1")
        assertEquals(1, cache.localIndexSize())

        cache.put("key2", "value2")
        // Key should be removed from index on eviction
        assertEquals(1, cache.localIndexSize())
    }

    // test for cache put many values
    @Test
    fun `should put many values`() {
        // Given
        val cache = givenSizedCache(2)

        // When
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")
        cache.put("key4", "value4")

        // Then
        assertEquals("value3", cache.get("key3"))
        assertEquals("value4", cache.get("key4"))
    }

    private fun givenSizedCache(size: Long): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig<String>(
            sizeEviction = WeightedEviction(size) { 1 },
            directExecution = true
        )
        return CaffeinePersistentCache.create(config)
    }

    private fun givenTimedCache(expireAfterAccess: Duration): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig<String>(
            sizeEviction = SizedEviction(Long.MAX_VALUE),
            expireAfterAccess = expireAfterAccess,
            directExecution = true
        )
        return CaffeinePersistentCache.create(config)
    }
}