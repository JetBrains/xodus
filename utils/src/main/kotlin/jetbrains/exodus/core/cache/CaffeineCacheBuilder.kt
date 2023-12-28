package jetbrains.exodus.core.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Weigher

internal object CaffeineCacheBuilder {

    /**
     * @param keyTransformer is required if Config key is of a different type than the key used in the cache.
     */
    fun <K, V, ConfigK> build(
        config: CaffeineCacheConfig<ConfigK, V>,
        keyTransformer: ((K) -> ConfigK)
    ): Cache<K, V> {
        return doBuild(config, keyTransformer)
    }

    fun <K, V> build(config: CaffeineCacheConfig<K, V>): Cache<K, V> {
        return doBuild(config)
    }

    private fun <K, V, ConfigK> doBuild(
        config: CaffeineCacheConfig<ConfigK, V>,
        keyTransformer: ((K) -> ConfigK)? = null
    ): Cache<K, V> {
        return Caffeine.newBuilder()
            // Size eviction
            .apply {
                val sizeEviction = config.sizeEviction
                when (sizeEviction) {
                    is FixedSizeEviction -> {
                        maximumSize(sizeEviction.maxSize)
                    }

                    is WeightSizeEviction -> {
                        maximumWeight(sizeEviction.maxWeight)
                        weigher(Weigher { key: K, value: V ->
                            @Suppress("UNCHECKED_CAST")
                            val key = keyTransformer?.invoke(key) ?: (key as ConfigK)
                            sizeEviction.weigher(key, value)
                        })
                    }
                }
            }
            // Time eviction
            .apply { if (config.expireAfterAccess != null) expireAfterAccess(config.expireAfterAccess) }
            // Reference eviction
            .apply { if (config.useSoftValues) softValues() }
            .apply { if (config.directExecution) executor(Runnable::run) }
            .build<K, V>()
    }
}