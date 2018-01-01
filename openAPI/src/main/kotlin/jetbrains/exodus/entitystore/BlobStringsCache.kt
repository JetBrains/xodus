/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore

import jetbrains.exodus.core.dataStructures.SoftLongObjectCache
import jetbrains.exodus.core.dataStructures.hash.HashUtil
import java.lang.ref.SoftReference
import java.util.*

internal class BlobStringsCache {

    private val cache = SoftLongObjectCache<Pair<Int, String>>(BLOB_STRING_CACHE_SIZE)
    private var bf = SoftReference<BitSet>(null)

    fun tryKey(blobVault: BlobVault, blobHandle: Long): String? {
        val cacheKey = cacheKey(blobVault, blobHandle)
        val (id, result) = cache.tryKey(cacheKey) ?: return null
        return if (id == blobVault.identity) result else null
    }

    fun getObject(blobVault: BlobVault, blobHandle: Long): String? {
        val cacheKey = cacheKey(blobVault, blobHandle)
        val (id, result) = cache.getObject(cacheKey) ?: return null
        return if (id == blobVault.identity) result else null
    }

    fun cacheObject(blobVault: BlobVault, blobHandle: Long, value: String) {
        val cacheKey = cacheKey(blobVault, blobHandle)
        val bit = (cacheKey % BLOOM_FILTER_SIZE).toInt()
        val filter = bloomFilter
        if (filter[bit]) {
            cache.cacheObject(cacheKey, blobVault.identity to value)
        } else {
            filter[bit] = true
        }
    }

    val hitRate: Float get() = cache.hitRate()

    private val bloomFilter: BitSet get() {
        var result = bf.get()
        if (result == null) {
            result = BitSet(BLOOM_FILTER_SIZE)
            bf = SoftReference(result)
        }
        return result
    }

    class BlobStringsCacheCreator {

        val instance = BlobStringsCache()
    }

    private companion object {

        val BLOB_STRING_CACHE_SIZE: Int = Integer.getInteger("exodus.entityStore.blobStringsCacheSize", 0x1000)
        val BLOOM_FILTER_SIZE = HashUtil.getCeilingPrime(BLOB_STRING_CACHE_SIZE)

        fun cacheKey(blobVault: BlobVault, blobHandle: Long) = blobHandle xor (blobVault.identity.toLong() shl 32)
    }
}