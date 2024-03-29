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
package jetbrains.exodus.core.cache.persistent

import jetbrains.exodus.core.cache.BasicCache

/**
 * This interface represents a cache that can store its previous versions,
 * so the update or delete of an entry in one version might not be visible in another.
 * The particular isolation guarantees are implementation-dependent.
 */
interface PersistentCache<K, V> : BasicCache<K, V> {

    /**
     * Current version of the cache.
     */
    val version: Long

    val externalIndex: PersistentIndex<K>?

    /**
     * Creates new version of the cache with the same configuration.
     */
    fun createNextVersion(): PersistentCache<K, V>

    /**
     * Register a client for the current version of the cache.
     * Returns a client that should be used to unregister the client to enable entries associated with its version
     * to be clean up later during the ordinary operations like get or put.
     *
     * Should be used when there several concurrent clients might use old versions of cache.
     *
     * Must be invoked once per client.
     *
     * Should be used in the way like:
     * ```
     * val client = cache.registerClient()
     * val nextCacheVersion = cache.createNextVersion()
     * try {
     *    // do something with the cache
     *    cache.get(key)
     *    cache.put(key, value)
     *    // ...
     * } finally {
     *   // As soon as client is unregistered, all values stored with its version are considered as stale
     *   client.unregister()
     * }
     */
    fun registerClient(): PersistentCacheClient

    /**
     * Release resources associated with the cache when it is no longer needed.
     *
     * Should be used in the way like:
     * ```
     * val cache = PersistentCacheImpl(...)
     * try {
     *   // do something with the cache
     *   cache.get(key)
     *   cache.put(key, value)
     *   // ...
     * } finally {
     *  cache.release()
     * }
     */
    fun release()
}

interface PersistentCacheClient {
    fun unregister()
}
