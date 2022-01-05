/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.util

import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache
import jetbrains.exodus.system.JVMConstants
import kotlin.math.min

class StringInterner private constructor(size: Int = INTERNER_SIZE) {

    private val cache: ConcurrentObjectCache<String, String>

    init {
        cache = ConcurrentObjectCache(size, NUMBER_OF_GENERATIONS)
    }

    fun doIntern(s: String?): String? {
        if (s == null || s.length > MAX_SIZE_OF_CACHED_STRING) return s
        val cached = cache.tryKey(s)
        if (cached != null) {
            return cached
        }
        val copy = if (JVMConstants.IS_JAVA8_OR_HIGHER) s else s + "" // to avoid large cached substrings
        cache.cacheObject(copy, copy)
        return copy
    }

    fun doIntern(builder: StringBuilder, maxLen: Int): String {
        val result = builder.toString()
        if (builder.length <= maxLen) {
            val cached = cache.tryKey(result)
            if (cached != null) {
                return cached
            }
            cache.cacheObject(result, result)
        }
        return result
    }

    companion object {

        private val NUMBER_OF_GENERATIONS = Integer.getInteger("exodus.util.stringInternerNumberOfGenerations", 5)
        private val MAX_SIZE_OF_CACHED_STRING = Integer.getInteger("exodus.util.stringInternerMaxEntrySize", 1000)
        private val INTERNER_SIZE = Integer.getInteger("exodus.util.stringInternerCacheSize", 15991 * NUMBER_OF_GENERATIONS)
        private val DEFAULT_INTERNER = StringInterner()

        @JvmStatic
        fun intern(s: String?) = DEFAULT_INTERNER.doIntern(s)

        @JvmStatic
        fun intern(builder: StringBuilder, maxLen: Int) = DEFAULT_INTERNER.doIntern(builder, min(maxLen, MAX_SIZE_OF_CACHED_STRING))

        @JvmStatic
        fun newInterner(size: Int) = StringInterner(size)
    }
}
