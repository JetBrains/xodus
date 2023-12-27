package jetbrains.exodus.core.dataStructures.cache

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
     * Returns a client that should be used to unregister the client and clean up entries associated with its version.
     */
    fun register(): CacheClient
}

interface CacheClient {
    fun unregister()
}
