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

abstract class ObjectCacheBase<K, V> protected constructor(size: Int) : CacheHitRateable() {
    protected val size: Int
    private val criticalSection: CriticalSection = CriticalSection { unlock() }

    init {
        this.size = Math.max(MIN_SIZE, size)
    }

    val isEmpty: Boolean
        get() = count() == 0

    fun size(): Int {
        return size
    }

    fun containsKey(key: K): Boolean {
        return isCached(key)
    }

    operator fun get(key: K): V {
        return tryKey(key)
    }

    fun isCached(key: K): Boolean {
        return getObject(key) != null
    }

    fun put(key: K, value: V): V? {
        val oldValue: V? = tryKey(key)
        if (oldValue != null) {
            remove(key)
        }
        cacheObject(key, value)
        return oldValue
    }

    open fun tryKeyLocked(key: K): V {
        newCriticalSection().use { ignored -> return tryKey(key) }
    }

    abstract fun clear()
    abstract fun lock()
    abstract fun unlock()
    abstract fun cacheObject(key: K, x: V): V

    // returns value pushed out of the cache
    abstract fun remove(key: K): V
    abstract fun tryKey(key: K): V
    abstract fun getObject(key: K): V?
    abstract fun count(): Int
    override fun adjustHitRate() {
        newCriticalSection().use { ignored -> super.adjustHitRate() }
    }

    open fun newCriticalSection(): CriticalSection? {
        lock()
        return criticalSection
    }

    interface CriticalSection : Closeable {
        override fun close()
    }

    companion object {
        const val DEFAULT_SIZE = 8192
        const val MIN_SIZE = 4
        val TRIVIAL_CRITICAL_SECTION: CriticalSection = CriticalSection {}

        /**
         * Formats hit rate in percent with one decimal place.
         *
         * @param hitRate hit rate value in the interval [0..1]
         */
        fun formatHitRate(hitRate: Float): String {
            val result = (hitRate * 1000).toInt()
            return (result / 10).toString() + '.' + result % 10 + '%'
        }
    }
}
