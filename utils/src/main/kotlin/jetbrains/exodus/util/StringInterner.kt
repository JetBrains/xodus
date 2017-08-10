/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.util

import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache

class StringInterner private constructor(size: Int = StringInterner.INTERNER_SIZE) {

    private val cache: ConcurrentObjectCache<String, String>

    init {
        cache = ConcurrentObjectCache<String, String>(size, NUMBER_OF_GENERATIONS)
    }

    fun doIntern(s: String?): String? {
        if (s == null) return null
        val cached = cache.tryKey(s)
        if (cached != null) {
            return cached
        }
        val copy = if (IS_JAVA8) s else s + "" // to avoid large cached substrings
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

        private const val NUMBER_OF_GENERATIONS = 3
        private val INTERNER_SIZE = Integer.getInteger("exodus.util.stringInternerCacheSize", 15991 * NUMBER_OF_GENERATIONS)
        private val IS_JAVA8 = System.getProperty("java.version").run { !startsWith("1.7") }
        private val DEFAULT_INTERNER = StringInterner()

        @JvmStatic
        fun intern(s: String?) = DEFAULT_INTERNER.doIntern(s)

        @JvmStatic
        fun intern(builder: StringBuilder, maxLen: Int) = DEFAULT_INTERNER.doIntern(builder, maxLen)

        @JvmStatic
        fun newInterner(size: Int) = StringInterner(size)
    }
}
