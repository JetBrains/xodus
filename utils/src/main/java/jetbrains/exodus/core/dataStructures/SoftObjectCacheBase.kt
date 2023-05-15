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
import java.lang.ref.SoftReference
import java.util.*

abstract class SoftObjectCacheBase<K, V> internal constructor(cacheSize: Int) : ObjectCacheBase<K, V>(cacheSize) {
    private val chunkSize: Int
    private val chunks: Array<SoftReference<ObjectCacheBase<K, V>?>>

    init {
        var cacheSize = cacheSize
        if (cacheSize < MIN_SIZE) {
            cacheSize = MIN_SIZE
        }
        chunks = arrayOfNulls<SoftReference<*>>(computeNumberOfChunks(cacheSize))
        chunkSize = cacheSize / chunks.size
        clear()
    }

    override fun clear() {
        Arrays.fill(chunks, null)
    }

    override fun lock() {}
    override fun unlock() {}
    override fun tryKey(key: K): V {
        incAttempts()
        val chunk = getChunk(key, false)
        val result = chunk?.tryKeyLocked(key)
        if (result != null) {
            incHits()
        }
        return result
    }

    override fun getObject(key: K): V {
        val chunk = getChunk(key, false) ?: return null
        chunk.newCriticalSection().use { ignored -> return chunk.getObject(key) }
    }

    override fun cacheObject(key: K, value: V): V {
        val chunk = getChunk(key, true)
        chunk!!.newCriticalSection().use { ignored -> return chunk.cacheObject(key, value) }
    }

    override fun remove(key: K): V {
        val chunk = getChunk(key, false) ?: return null
        chunk.newCriticalSection().use { ignored -> return chunk.remove(key) }
    }

    override fun count(): Int {
        throw UnsupportedOperationException()
    }

    override fun newCriticalSection(): CriticalSection? {
        return ObjectCacheBase.Companion.TRIVIAL_CRITICAL_SECTION
    }

    protected abstract fun newChunk(chunkSize: Int): ObjectCacheBase<K, V>
    private fun getChunk(key: K, create: Boolean): ObjectCacheBase<K, V>? {
        val hc = key.hashCode()
        val chunkIndex = (hc + (hc shr 31) and 0x7fffffff) % chunks.size
        val ref = chunks[chunkIndex]
        var result = if (ref == null) null else ref.get()
        if (result == null && create) {
            result = newChunk(chunkSize)
            chunks[chunkIndex] = SoftReference(result)
        }
        return result
    }

    companion object {
        const val MIN_SIZE = 16
        fun computeNumberOfChunks(cacheSize: Int): Int {
            var result = Math.sqrt(cacheSize.toDouble()).toInt()
            while (result * result < cacheSize) {
                ++result
            }
            return HashUtil.getCeilingPrime(result)
        }
    }
}
