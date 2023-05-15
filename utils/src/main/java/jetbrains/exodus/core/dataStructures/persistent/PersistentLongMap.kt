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

interface PersistentLongMap<V> {
    fun beginRead(): ImmutableMap<V>
    val clone: PersistentLongMap<V>
    fun beginWrite(): MutableMap<V>
    interface ImmutableMap<V> : Iterable<Entry<V>?> {
        operator fun get(key: Long): V?
        fun containsKey(key: Long): Boolean
        val isEmpty: Boolean
        fun size(): Int
        val minimum: Entry<V?>?
        fun reverseIterator(): Iterator<Entry<V>>
        fun tailEntryIterator(staringKey: Long): Iterator<Entry<V?>?>?
        fun tailReverseEntryIterator(staringKey: Long): Iterator<Entry<V?>?>?
    }

    interface MutableMap<V> : ImmutableMap<V> {
        fun put(key: Long, value: V)
        fun remove(key: Long): V
        fun clear()
        fun endWrite(): Boolean
        fun testConsistency() // for testing consistency
    }

    interface Entry<V> : Comparable<Entry<V>?> {
        val key: Long
        val value: V
    }
}
