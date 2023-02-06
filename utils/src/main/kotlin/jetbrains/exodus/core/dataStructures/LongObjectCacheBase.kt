/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures

import java.io.Closeable
import kotlin.math.max

abstract class LongObjectCacheBase<V> protected constructor(size: Int) : CacheHitRateable() {

    companion object {
        const val DEFAULT_SIZE = 8192
        const val MIN_SIZE = 4
    }

    private val size = max(MIN_SIZE, size)
    private val criticalSection: Closeable = object : Closeable {
        override fun close() {
            unlock()
        }
    }

    val isEmpty: Boolean get() = count() == 0

    fun containsKey(key: Long) = isCached(key)

    operator fun get(key: Long) = tryKey(key)

    fun put(key: Long, value: V): V? {
        val oldValue: V? = getObject(key)
        if (oldValue != null) {
            remove(key)
        }
        cacheObject(key, value)
        return oldValue
    }

    open fun tryKeyLocked(key: Long) = newCriticalSection().use { tryKey(key) }

    open fun getObjectLocked(key: Long) = newCriticalSection().use { getObject(key) }

    open fun cacheObjectLocked(key: Long, x: V) = newCriticalSection().use {
        if (getObject(key) == null) cacheObject(key, x) else null
    }

    open fun removeLocked(key: Long) = newCriticalSection().use { remove(key) }

    fun isCached(key: Long) = getObjectLocked(key) != null

    fun size() = size

    override fun adjustHitRate() = newCriticalSection().use { super.adjustHitRate() }

    abstract fun clear()

    abstract fun lock()

    abstract fun unlock()

    abstract fun tryKey(key: Long): V?

    abstract fun getObject(key: Long): V?

    abstract fun cacheObject(key: Long, x: V): V?

    // returns value pushed out of the cache
    abstract fun remove(key: Long): V?

    abstract fun count(): Int

    private fun newCriticalSection(): Closeable {
        lock()
        return criticalSection
    }
}
