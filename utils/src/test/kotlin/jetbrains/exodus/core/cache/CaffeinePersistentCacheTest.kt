package jetbrains.exodus.core.dataStructures.cache

import jetbrains.exodus.core.cache.CaffeineCacheConfig
import jetbrains.exodus.core.cache.FixedSizeEviction
import jetbrains.exodus.core.cache.WeightSizeEviction
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
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
        cache2.forceEviction()

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
        cache2.put("key", "v")
        cache2.forceEviction()

        // Then
        assertEquals(1, cache2.count())
        assertEquals("v", cache2.get("key"))
    }

    @Test
    fun `should evict based on time`() {
        // Given
        val cache1 = givenTimedCache(Duration.ofMillis(0))
        val cache2 = cache1.createNextVersion()

        // When
        cache1.put("key", "value1")
        cache2.put("key", "value2")
        cache2.forceEviction()

        // Then
        assertEquals(0, cache2.count())
    }

    private fun givenSizedCache(size: Long = 10): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig<String, String>(sizeEviction = FixedSizeEviction(size))
        return CaffeinePersistentCache.create(config)
    }

    private fun givenWeightedCache(
        weight: Long = 10,
        weigher: (String, String) -> Int
    ): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig(sizeEviction = WeightSizeEviction(weight, weigher))
        return CaffeinePersistentCache.create(config)
    }

    private fun givenTimedCache(expireAfterAccess: Duration): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig<String, String>(
            sizeEviction = FixedSizeEviction(Long.MAX_VALUE),
            expireAfterAccess = expireAfterAccess
        )
        return CaffeinePersistentCache.create(config)
    }
}