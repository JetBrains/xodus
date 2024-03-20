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

import java.util.function.Consumer

interface BasicCache<K, V> {

    /**
     * Returns the maximum number of elements in the cache or max weight of the cache
     * depending on which configuration is used.
     */
    fun size(): Long

    /**
     * Sets the maximum number of elements in the cache or max weight of the cache.
     *
     * Exactly what this means is implementation-dependent.
     */
    fun setSize(size: Long)

    /**
     * Returns true if the cache is weighted.
     */
    fun isWeighted(): Boolean = false

    /**
     * Returns the current number of elements in the cache.
     */
    fun count(): Long

    /**
     * Returns the value associated with the key, or null if there is no such value.
     */
    fun get(key: K): V?

    /**
     * Store value in cache by key.
     */
    fun put(key: K, value: V)

    /**
     * Invalidate value by key if present.
     */
    fun remove(key: K)

    /**
     * Removes all data from the cache.
     */
    fun clear()

    /**
     * Forces cache to evict stale entries.
     *
     * Exactly which activities are performed (if any) is implementation-dependent.
     */
    fun forceEviction()

    /**
     * Apply consumer to each entry in the cache. It's assumed that neither key nor value will be modified.
     */
    fun forEachKey(consumer: Consumer<K>)
}