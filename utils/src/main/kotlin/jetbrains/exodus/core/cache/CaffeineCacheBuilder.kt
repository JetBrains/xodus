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
package jetbrains.exodus.core.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine

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
                        weigher { key: K, value: V ->
                            @Suppress("UNCHECKED_CAST")
                            val key = keyTransformer?.invoke(key) ?: (key as ConfigK)
                            sizeEviction.weigher(key, value)
                        }
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