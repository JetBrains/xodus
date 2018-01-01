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
package jetbrains.exodus.core.dataStructures.persistent

inline fun <K : Comparable<K>, V, R> Persistent23TreeMap<K, V>.read(block: Persistent23TreeMap.ImmutableMap<K, V>.() -> R): R {
    return beginRead().block()
}

inline fun <K : Comparable<K>, V> Persistent23TreeMap<K, V>.write(block: Persistent23TreeMap.MutableMap<K, V>.() -> Unit): Boolean {
    val mutableMap = beginWrite()
    mutableMap.block()
    return mutableMap.endWrite()
}

inline fun <K : Comparable<K>, V> Persistent23TreeMap<K, V>.writeFinally(block: Persistent23TreeMap.MutableMap<K, V>.() -> Unit) {
    while (!write(block)) {
    }
}

inline fun <V, R> PersistentLongMap<V>.read(block: PersistentLongMap.ImmutableMap<V>.() -> R): R {
    return beginRead().block()
}

inline fun <V> PersistentLongMap<V>.write(block: PersistentLongMap.MutableMap<V>.() -> Unit): Boolean {
    val mutableMap = beginWrite()
    mutableMap.block()
    return mutableMap.endWrite()
}

inline fun <V> PersistentLongMap<V>.writeFinally(block: PersistentLongMap.MutableMap<V>.() -> Unit) {
    while (!write(block)) {
    }
}

inline fun <R> PersistentLongSet.read(block: PersistentLongSet.ImmutableSet.() -> R): R {
    return beginRead().block()
}

inline fun PersistentLongSet.write(block: PersistentLongSet.MutableSet.() -> Unit): Boolean {
    val mutableMap = beginWrite()
    mutableMap.block()
    return mutableMap.endWrite()
}

inline fun PersistentLongSet.writeFinally(block: PersistentLongSet.MutableSet.() -> Unit) {
    while (!write(block)) {
    }
}

inline fun <K, R> PersistentHashSet<K>.read(block: PersistentHashSet.ImmutablePersistentHashSet<K>.() -> R): R {
    return beginRead().block()
}

inline fun <K> PersistentHashSet<K>.write(block: PersistentHashSet.MutablePersistentHashSet<K>.() -> Unit): Boolean {
    val mutableSet = beginWrite()
    mutableSet.block()
    return mutableSet.endWrite()
}

inline fun <K> PersistentHashSet<K>.writeFinally(block: PersistentHashSet.MutablePersistentHashSet<K>.() -> Unit) {
    while (!write(block)) {
    }
}

inline fun <K, V, R> PersistentHashMap<K, V>.read(block: PersistentHashMap<K, V>.ImmutablePersistentHashMap.() -> R): R {
    return current.block()
}

inline fun <K, V> PersistentHashMap<K, V>.write(block: PersistentHashMap<K, V>.MutablePersistentHashMap.() -> Unit): Boolean {
    val mutableMap = beginWrite()
    mutableMap.block()
    return mutableMap.endWrite()
}

inline fun <K, V> PersistentHashMap<K, V>.writeFinally(block: PersistentHashMap<K, V>.MutablePersistentHashMap.() -> Unit) {
    while (!write(block)) {
    }
}

inline fun <K, V, R> PersistentLinkedHashMap<K, V>.read(block: PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V>.() -> R): R {
    return beginWrite().block()
}

inline fun <K, V> PersistentLinkedHashMap<K, V>.write(block: PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V>.() -> Unit): Boolean {
    val mutableMap = beginWrite()
    mutableMap.block()
    return endWrite(mutableMap)
}

inline fun <K, V> PersistentLinkedHashMap<K, V>.writeFinally(block: PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V>.() -> Unit) {
    while (!write(block)) {
    }
}
