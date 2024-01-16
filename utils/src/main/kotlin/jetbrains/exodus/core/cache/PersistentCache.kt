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

import java.util.function.BiConsumer

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

    /**
     * Creates new version of the cache with the same configuration.
     */
    fun createNextVersion(entryConsumer: BiConsumer<K, V>? = null): PersistentCache<K, V>

    /**
     * Register a client for the current version of the cache.
     * Returns a client that should be used to unregister the client to enable entries associated with its version
     * to be clean up later during the ordinary operations like get or put.
     */
    fun register(): CacheClient
}

interface CacheClient {
    fun unregister()
}
