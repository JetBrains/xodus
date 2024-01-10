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
package jetbrains.exodus.core.dataStructures.cache

import jetbrains.exodus.core.cache.CaffeineCacheConfig
import jetbrains.exodus.core.cache.CaffeinePersistentCache
import jetbrains.exodus.core.cache.FixedSizeEviction
import jetbrains.exodus.core.cache.WeightSizeEviction
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Duration

class CaffeinePersistentCacheTest {

    @Test
    fun `should be versioned when put`() {
        // Given
        val cache1 = givenSizedCache(10)
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key", "value1")
        cache2.put("key", "value2")
        val value1 = cache1.get("key")
        val value2 = cache2.get("key")

        // Then
        assertEquals("value1", value1)
        assertEquals("value2", value2)
    }

    @Test
    fun `should be versioned when remove`() {
        // Given
        val cache1 = givenSizedCache(10)
        val cache2 = cache1.createNextVersion()
        val cache3 = cache2.createNextVersion()

        // When
        cache1.put("key", "value")
        cache2.remove("key")
        val value1 = cache1.get("key")
        val value2 = cache2.get("key")
        val value3 = cache3.get("key")

        // Then
        assertEquals("value", value1)
        assertNull(value2)
        assertNull(value3)
    }

    @Test
    fun `should evict based on size`() {
        // Given
        val cache1 = givenSizedCache(1)
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key", "value1")
        cache2.put("key", "value2")

        // Then
        assertEquals(1, cache2.count())
        assertEquals("value2", cache2.get("key"))
    }

    @Test
    fun `should evict based on weight`() {
        // Given
        val cache1 = givenWeightedCache(2, { _, value -> value.length })
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key", "v1")
        cache2.put("key", "v2")

        // Then
        assertEquals(1, cache2.count())
        // assert either of the values is present in cache as the eviction is not deterministic
        assertTrue(cache1.get("key") == "v1" || cache2.get("key") == "v2")
    }

    @Test
    fun `should evict based on time`() {
        // Given
        val cache1 = givenTimedCache(Duration.ofMillis(1))
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key", "value1")
        cache2.put("key", "value2")
        Thread.sleep(2)
        val value1 = cache1.get("key")
        val value2 = cache2.get("key")

        // Then
        assertEquals(0, cache2.count())
        assertNull(value1)
        assertNull(value2)
    }

    @Test
    fun `should copy entries to new version`() {
        // Given
        val cache1 = givenSizedCache(4) // 2 entries for each version
        cache1.put("key1", "value1")
        cache1.put("key2", "value2")
        val cache2 = cache1.createNextVersion()

        // When
        val value1 = cache2.get("key1")
        val value2 = cache2.get("key2")

        // Then
        assertEquals(4, cache2.count())
        assertEquals("value1", value1)
        assertEquals("value2", value2)

    }

    @Test
    fun `should evict when client unregisters`() {
        // Given
        val cache1 = givenSizedCache(1)
        val cache2 = cache1.createNextVersion()

        // When
        val registration = cache1.register()
        cache1.put("key", "value")
        registration.unregister()

        // Then
        assertEquals(0, cache2.count())
    }

    private fun givenSizedCache(size: Long): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig<String, String>(
            sizeEviction = FixedSizeEviction(size),
            directExecution = true
        )
        return CaffeinePersistentCache.create(config)
    }

    private fun givenWeightedCache(
        weight: Long,
        weigher: (String, String) -> Int
    ): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig(
            sizeEviction = WeightSizeEviction(weight, weigher),
            directExecution = true
        )
        return CaffeinePersistentCache.create(config)
    }

    private fun givenTimedCache(expireAfterAccess: Duration): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig<String, String>(
            sizeEviction = FixedSizeEviction(Long.MAX_VALUE),
            expireAfterAccess = expireAfterAccess,
            directExecution = true
        )
        return CaffeinePersistentCache.create(config)
    }
}