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
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.hash.LongIterator

interface PersistentLongSet {
    fun beginRead(): ImmutableSet
    @JvmField
    val clone: PersistentLongSet
    fun beginWrite(): MutableSet
    interface ImmutableSet {
        operator fun contains(key: Long): Boolean
        fun longIterator(): LongIterator
        fun reverseLongIterator(): LongIterator
        fun tailLongIterator(key: Long): LongIterator
        fun tailReverseLongIterator(key: Long): LongIterator
        val isEmpty: Boolean
        fun size(): Int
    }

    interface MutableSet : ImmutableSet {
        fun add(key: Long)
        fun remove(key: Long): Boolean
        fun clear()
        fun endWrite(): Boolean
    }
}
