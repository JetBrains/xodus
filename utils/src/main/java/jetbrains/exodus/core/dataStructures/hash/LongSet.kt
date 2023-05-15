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
package jetbrains.exodus.core.dataStructures.hash

import java.util.*

interface LongSet : MutableSet<Long?> {
    operator fun contains(key: Long): Boolean
    fun add(key: Long): Boolean
    fun remove(key: Long): Boolean
    override fun iterator(): LongIterator
    fun toLongArray(): LongArray
    class EmptySet : AbstractSet<Long?>(), LongSet {
        override fun contains(key: Long): Boolean {
            return false
        }

        override fun add(key: Long): Boolean {
            throw UnsupportedOperationException()
        }

        override fun remove(key: Long): Boolean {
            throw UnsupportedOperationException()
        }

        override fun iterator(): LongIterator {
            return LongIterator.Companion.EMPTY
        }

        override fun toLongArray(): LongArray {
            return EMPTY_ARRAY
        }

        override fun size(): Int {
            return 0
        }
    }

    companion object {
        @JvmField
        val EMPTY: LongSet = EmptySet()
        val EMPTY_ARRAY = LongArray(0)
    }
}
