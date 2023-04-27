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
package jetbrains.exodus.env

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.core.dataStructures.ConcurrentLongObjectCache
import jetbrains.exodus.core.dataStructures.SoftConcurrentLongObjectCache
import jetbrains.exodus.core.execution.SharedTimer.ExpirablePeriodicTask

/**
 * Caches Store.get() results retrieved from immutable trees.
 * For each key and tree address (KeyEntry), value is immutable, so lock-free caching is ok.
 */
class StoreGetCache(cacheSize: Int, val minTreeSize: Int, val maxValueSize: Int) {
    private val cache: SoftConcurrentLongObjectCache<ValueEntry>

    init {
        cache = object : SoftConcurrentLongObjectCache<ValueEntry>(cacheSize) {
            override fun newChunk(chunkSize: Int): ConcurrentLongObjectCache<ValueEntry?> {
                return object : ConcurrentLongObjectCache<ValueEntry?>(chunkSize, SINGLE_CHUNK_GENERATIONS) {
                    override fun getCacheAdjuster(): ExpirablePeriodicTask? {
                        return null
                    }
                }
            }
        }
    }

    fun close() {
        cache.close()
    }

    fun tryKey(treeRootAddress: Long, key: ByteIterable): ByteIterable? {
        val keyHashCode = key.hashCode()
        val ve = cache.tryKey(treeRootAddress xor keyHashCode.toLong())
        return if (ve == null || ve.treeRootAddress != treeRootAddress || ve.keyHashCode != keyHashCode || ve.key != key) null else ve.value
    }

    fun cacheObject(treeRootAddress: Long, key: ByteIterable, value: ArrayByteIterable) {
        val keyCopy = if (key is ArrayByteIterable) key else ArrayByteIterable(key)
        val keyHashCode = keyCopy.hashCode()
        cache.cacheObject(
            treeRootAddress xor keyHashCode.toLong(),
            ValueEntry(treeRootAddress, keyHashCode, keyCopy, value)
        )
    }

    fun hitRate(): Float {
        return cache.hitRate()
    }

    private class ValueEntry(
        val treeRootAddress: Long,
        val keyHashCode: Int,
        val key: ArrayByteIterable,
        val value: ArrayByteIterable
    )

    companion object {
        private const val SINGLE_CHUNK_GENERATIONS = 4
    }
}
