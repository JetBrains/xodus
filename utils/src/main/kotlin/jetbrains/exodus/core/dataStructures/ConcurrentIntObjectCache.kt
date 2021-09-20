/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@file:Suppress("UNCHECKED_CAST")

package jetbrains.exodus.core.dataStructures

import jetbrains.exodus.core.dataStructures.hash.HashUtil
import jetbrains.exodus.util.MathUtil

open class ConcurrentIntObjectCache<V>
@JvmOverloads constructor(size: Int = DEFAULT_SIZE,
                          private val numberOfGenerations: Int = DEFAULT_NUMBER_OF_GENERATIONS) : IntObjectCacheBase<V>(size) {

    companion object {
        const val DEFAULT_NUMBER_OF_GENERATIONS = 3
        private val NULL_OBJECT: CacheEntry<*> = CacheEntry(Int.MIN_VALUE, null)
    }

    private val generationSize = HashUtil.getFloorPrime(size / numberOfGenerations)
    private val mask = (1 shl MathUtil.integerLogarithm(generationSize)) - 1
    private val cache = Array(numberOfGenerations * generationSize) { NULL_OBJECT } as Array<CacheEntry<V?>>

    override fun tryKeyLocked(key: Int) = tryKey(key)

    override fun getObjectLocked(key: Int) = getObject(key)

    override fun cacheObjectLocked(key: Int, x: V) = cacheObject(key, x)

    override fun removeLocked(key: Int) = remove(key)

    override fun clear() {
        for (i in cache.indices) {
            cache[i] = NULL_OBJECT as CacheEntry<V?>
        }
    }

    override fun lock() {}

    override fun unlock() {}

    override fun cacheObject(key: Int, x: V): V? {
        var cacheIndex = indexFor(key)
        repeat(numberOfGenerations) {
            val entry = cache[cacheIndex]
            if (entry.key == key) {
                cache[cacheIndex] = CacheEntry(key, x)
                // in highly concurrent environment we can't definitely know if a value is pushed out from the cache
                return null
            }
            ++cacheIndex
        }
        cache[cacheIndex - 1] = CacheEntry(key, x)
        return null
    }

    override fun remove(key: Int): V? {
        var cacheIndex = indexFor(key)
        repeat(numberOfGenerations) {
            val entry = cache[cacheIndex]
            if (entry.key == key) {
                val result = entry.value
                entry.value = null
                return result
            }
            ++cacheIndex
        }
        return null
    }

    override fun tryKey(key: Int): V? {
        incAttempts()
        var cacheIndex = indexFor(key)
        var entry = cache[cacheIndex]
        if (entry.key == key) {
            incHits()
            return entry.value
        }
        repeat(numberOfGenerations - 1) {
            entry = cache[++cacheIndex]
            if (entry.key == key) {
                incHits()
                val temp = cache[cacheIndex - 1]
                cache[cacheIndex - 1] = entry
                cache[cacheIndex] = temp
                return entry.value
            }
        }
        return null
    }

    override fun getObject(key: Int): V? {
        var cacheIndex = indexFor(key)
        repeat(numberOfGenerations) {
            val entry = cache[cacheIndex]
            if (entry.key == key) {
                return entry.value
            }
            ++cacheIndex
        }
        return null
    }

    override fun count(): Int = throw UnsupportedOperationException()

    private fun indexFor(key: Int) = HashUtil.indexFor(key, generationSize, mask) * numberOfGenerations

    private class CacheEntry<V>(val key: Int, var value: V?) {}
}
