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

/**
 * This interface represents a persistent index of keys that might be used by the cache client.
 * For example, when a client needs to find all entries to invalidate by value they might have reversed index (value -> keys) might be used.

 */
interface PersistentIndex<K> {

    /**
     * Adds a key to the index.
     */
    fun add(key: K)

    /**
     * Removes a key from the index.
     */
    fun remove(key: K)

    /**
     * Should return a persistent clone of the current index.
     * This method is aligned with the PersistentCache interface and is used to create a new cache instance with the same index.
     */
    fun clone(): PersistentIndex<K>
}