package jetbrains.exodus.core.dataStructures.cache

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class CaffeinePersistentCacheTest {

    @Test
    fun `should be versioned when put`() {
        // Given
        val cache1 = givenCache(10)
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
        val cache1 = givenCache(10)
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

    private fun givenCache(size: Long = 10): CaffeinePersistentCache<String, String> {
        val config = CaffeineCacheConfig(maxSize = size)
        return CaffeinePersistentCache.createSized(config)
    }
}