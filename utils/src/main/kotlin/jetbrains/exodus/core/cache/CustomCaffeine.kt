package jetbrains.exodus.core.cache

import com.github.benmanes.caffeine.cache.Cache
import java.util.concurrent.atomic.AtomicLong

class CustomCaffeine<K, V>(private val cache: Cache<K, V>) : Cache<K, V> by cache {

    // This is a workaround to get maximum size of the cache quickly without locking
    // as it is implemented in Caffeine as of time being.
    private var sizeRef = AtomicLong(cache.policy().eviction().orElseThrow().maximum)

    fun trySetSize(size: Long): Boolean {
        val old = this.sizeRef.toLong()
        if (this.sizeRef.compareAndSet(old, size)) {
            cache.policy().eviction().orElseThrow().maximum = size
            return true
        }
        return false
    }

    fun getSize(): Long {
        return sizeRef.get()
    }
}