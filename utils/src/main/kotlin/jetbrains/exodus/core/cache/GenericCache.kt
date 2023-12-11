package jetbrains.exodus.core.dataStructures.cache

import java.util.function.BiConsumer

interface GenericCache<K, V> {

    /**
     * Returns the maximum number of elements in the cache.
     */
    fun size(): Int

    /**
     * Returns the current number of elements in the cache.
     */
    fun count(): Int

    /**
     * Returns the value associated with the key, or null if there is no such value.
     */
    fun get(key: K): V?

    /**
     * Store value in cache by key.
     */
    fun put(key: K, v: V)

    /**
     * Invalidate value by key if present.
     */
    fun remove(key: K)

    /**
     * Removes all data from the cache.
     */
    fun clear()

    /**
     * Apply consumer to each entry in the cache. It's assumed that neither key nor value will be modified.
     */
    fun forEachEntry(consumer: BiConsumer<K, V>)
}