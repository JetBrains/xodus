package jetbrains.exodus.core.cache

import java.time.Duration


data class CaffeineCacheConfig<K, V>(
    val sizeEviction: SizeEviction<K, V>,
    val expireAfterAccess: Duration? = null,
    val useSoftValues: Boolean = true,
)

sealed interface SizeEviction<K, V> {
    val size: Long
}

data class FixedSizeEviction<K, V>(val maxSize: Long) : SizeEviction<K, V> {
    override val size: Long = maxSize
}

data class WeightSizeEviction<K, V>(
    val maxWeight: Long = -1,
    val weigher: (K, V) -> Int
) : SizeEviction<K, V> {
    override val size: Long = maxWeight
}