package jetbrains.exodus.core.cache.persistent

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

typealias ValueWeigher<V> = (V) -> Int

// Thread-safe container for versioned cache entry value that holds total weight of all values collectively
internal class WeightedValueMap<K, V>(private val weigher: ValueWeigher<V>) {

    private val map = ConcurrentHashMap<K, V>()

    // Total weight of all values collectively
    private val totalWeightRef = AtomicInteger()

    val size get() = map.size
    val keys get() = map.keys
    val totalWeight get() = totalWeightRef.get()

    fun put(key: K, value: V) {
        map.compute(key) { _, prevValue ->
            val toSubtract = prevValue?.let(weigher) ?: 0
            // Coersing is needed to avoid negative values as weigher can return different values for the same value
            totalWeightRef.updateAndGet { (it + weigher(value) - toSubtract).coerceAtLeast(0) }
            value
        }
    }

    fun remove(key: K) {
        map.computeIfPresent(key) { _, value ->
            totalWeightRef.updateAndGet { (it - weigher(value)).coerceAtLeast(0) }
            null
        }
    }

    fun get(key: K): V? {
        return map[key]
    }

    fun orNullIfEmpty(): WeightedValueMap<K, V>? {
        return if (map.isEmpty()) null else this
    }
}