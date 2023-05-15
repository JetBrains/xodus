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

import jetbrains.exodus.core.dataStructures.hash.*
import jetbrains.exodus.util.*

open class ConcurrentObjectCache<K, V> @JvmOverloads constructor(
    size: Int = ObjectCacheBase.Companion.DEFAULT_SIZE,
    private val numberOfGenerations: Int = DEFAULT_NUMBER_OF_GENERATIONS
) : ObjectCacheBase<K, V>(size) {
    private val generationSize: Int
    private val mask: Int
    private val cache: Array<CacheEntry<K, V>>

    init {
        generationSize = HashUtil.getFloorPrime(size / numberOfGenerations)
        mask = (1 shl MathUtil.integerLogarithm(generationSize)) - 1
        cache = arrayOfNulls<CacheEntry<*, *>>(
            numberOfGenerations * generationSize
        )
    }

    override fun tryKeyLocked(key: K): V {
        return tryKey(key)
    }

    override fun clear() {
        // do nothing
    }

    override fun lock() {}
    override fun unlock() {}
    override fun cacheObject(key: K, x: V): V {
        var cacheIndex = HashUtil.indexFor(key.hashCode(), generationSize, mask) * numberOfGenerations
        var i = 0
        while (i < numberOfGenerations) {
            val entry = cache[cacheIndex]
            if (entry != null && entry.key == key) {
                cache[cacheIndex] = CacheEntry(key, x)
                // in concurrent environment we can't definitely know if a value is pushed out from the cache
                return null
            }
            ++i
            ++cacheIndex
        }
        cache[cacheIndex - 1] = CacheEntry(key, x)
        return null
    }

    override fun remove(key: K): V {
        var cacheIndex = HashUtil.indexFor(key.hashCode(), generationSize, mask) * numberOfGenerations
        var i = 0
        while (i < numberOfGenerations) {
            val entry = cache[cacheIndex]
            if (entry != null && entry.key == key) {
                val result: V = entry.value
                entry.value = null
                return result
            }
            ++i
            ++cacheIndex
        }
        return null
    }

    override fun tryKey(key: K): V {
        incAttempts()
        var cacheIndex = HashUtil.indexFor(key.hashCode(), generationSize, mask) * numberOfGenerations
        var entry = cache[cacheIndex]
        if (entry != null && entry.key == key) {
            incHits()
            return entry.value
        }
        for (i in 1 until numberOfGenerations) {
            entry = cache[++cacheIndex]
            if (entry != null && entry.key == key) {
                incHits()
                val temp = cache[cacheIndex - 1]
                cache[cacheIndex - 1] = entry
                cache[cacheIndex] = temp
                return entry.value
            }
        }
        return null
    }

    override fun getObject(key: K): V {
        var cacheIndex = HashUtil.indexFor(key.hashCode(), generationSize, mask) * numberOfGenerations
        var i = 0
        while (i < numberOfGenerations) {
            val entry = cache[cacheIndex]
            if (entry != null && entry.key == key) {
                return entry.value
            }
            ++i
            ++cacheIndex
        }
        return null
    }

    override fun count(): Int {
        throw UnsupportedOperationException()
    }

    override fun newCriticalSection(): CriticalSection? {
        return ObjectCacheBase.Companion.TRIVIAL_CRITICAL_SECTION
    }

    private class CacheEntry<K, V>(val key: K, val value: V?)
    companion object {
        const val DEFAULT_NUMBER_OF_GENERATIONS = 3
    }
}
