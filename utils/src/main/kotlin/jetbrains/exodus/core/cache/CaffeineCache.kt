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
import java.util.function.BiConsumer

class CaffeineCache<K, V>(
    private val config: CaffeineCacheConfig,
    private val cache: Cache<K, V>
) : BasicCache<K, V> {

    companion object {

        fun <K, V> create(size: Int): CaffeineCache<K, V> {
            val config = CaffeineCacheConfig(
                maxSize = size.toLong()
            )
            return create(config)
        }

        fun <K, V> create(config: CaffeineCacheConfig): CaffeineCache<K, V> {
            val cache = Caffeine.newBuilder()
                .withConfig(config)
                .apply { maximumSize(config.maxSize) }
                .build<K, V>()
            return CaffeineCache(config, cache)
        }
    }

    override fun size(): Long {
        return config.maxSize
    }

    override fun count(): Long {
        return cache.estimatedSize()
    }

    override fun get(key: K): V? {
        return cache.getIfPresent(key)
    }

    override fun put(key: K, value: V) {
        cache.put(key, value)
    }

    override fun remove(key: K) {
        cache.invalidate(key)
    }

    override fun clear() {
        cache.invalidateAll()
    }

    override fun forceEviction() {
        cache.cleanUp()
    }

    override fun forEachEntry(consumer: BiConsumer<K, V>) {
        cache.asMap().forEach(consumer)
    }
}