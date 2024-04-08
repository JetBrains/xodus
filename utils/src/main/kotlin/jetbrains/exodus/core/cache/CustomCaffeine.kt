package jetbrains.exodus.core.cache

import com.github.benmanes.caffeine.cache.Cache
import java.util.concurrent.atomic.AtomicLong

class CustomCaffeine<K, V>(private val cache: Cache<K, V>) : Cache<K, V> by cache {

    // This is a workaround to get maximum size of the cache quickly without locking
    // as it is implemented in Caffeine as of time being.
    private var sizeRef = AtomicLong(cache.policy().eviction().orElseThrow().maximum)

    fun trySetSize(targetSize: Long): Boolean {
        val currentSize = sizeRef.get()
        if (this.sizeRef.compareAndSet(currentSize, targetSize)) {
            if (currentSize == targetSize) {
                // Size is not actually changed
                return false
            } else {
                cache.policy().eviction().orElseThrow().maximum = targetSize
                return true
            }
        }
        return false
    }

    fun getSize(): Long {
        return sizeRef.get()
    }
}