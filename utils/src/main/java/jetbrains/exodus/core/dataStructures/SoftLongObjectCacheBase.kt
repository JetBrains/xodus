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

import java.lang.ref.SoftReference
import java.util.*

abstract class SoftLongObjectCacheBase<V> @JvmOverloads constructor(cacheSize: Int = DEFAULT_SIZE) :
    LongObjectCacheBase<V>(cacheSize) {
    private val chunks: Array<SoftReference<LongObjectCacheBase<V>>>
    private val chunkSize: Int

    init {
        var cacheSize = cacheSize
        if (cacheSize < MIN_SIZE) {
            cacheSize = MIN_SIZE
        }
        chunks = arrayOfNulls<SoftReference<*>>(SoftObjectCacheBase.Companion.computeNumberOfChunks(cacheSize))
        chunkSize = cacheSize / chunks.size
        clear()
    }

    override fun clear() {
        Arrays.fill(chunks, null)
    }

    override fun lock() {}
    override fun unlock() {}
    override fun tryKey(key: Long): V? {
        incAttempts()
        val chunk = getChunk(key, false)
        val result = chunk?.tryKeyLocked(key)
        if (result != null) {
            incHits()
        }
        return result
    }

    override fun getObject(key: Long): V? {
        val chunk = getChunk(key, false) ?: return null
        return chunk.getObjectLocked(key)
    }

    override fun cacheObject(key: Long, value: V): V? {
        val chunk = getChunk(key, true) ?: throw NullPointerException()
        return chunk.cacheObjectLocked(key, value)
    }

    override fun remove(key: Long): V? {
        val chunk = getChunk(key, false) ?: return null
        return chunk.removeLocked(key)
    }

    override fun count(): Int {
        throw UnsupportedOperationException()
    }

    protected abstract fun newChunk(chunkSize: Int): LongObjectCacheBase<V>
    private fun getChunk(key: Long, create: Boolean): LongObjectCacheBase<V>? {
        val chunkIndex = ((key and 0x7fffffffffffffffL) % chunks.size).toInt()
        val ref = chunks[chunkIndex]
        var result = if (ref == null) null else ref.get()
        if (result == null && create) {
            result = newChunk(chunkSize)
            chunks[chunkIndex] = SoftReference(result)
        }
        return result
    }

    companion object {
        const val DEFAULT_SIZE = 4096
        const val MIN_SIZE = 16
    }
}
