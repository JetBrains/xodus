package jetbrains.exodus.core.dataStructures.cache

import java.util.function.BiConsumer

interface PersistentCache<K, V> : GenericCache<K, V> {

    val currentVersion: Long

    fun createNextVersion(entryConsumer: BiConsumer<K,V>? = null): PersistentCache<K,V>
}
