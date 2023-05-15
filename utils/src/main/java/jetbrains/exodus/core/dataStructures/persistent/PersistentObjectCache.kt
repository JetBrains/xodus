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

import jetbrains.exodus.core.dataStructures.CacheHitRateable
import jetbrains.exodus.core.dataStructures.LongObjectCacheBase.size
import jetbrains.exodus.core.dataStructures.ObjectCacheBase
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.hash.*
import jetbrains.exodus.core.dataStructures.persistent.PersistentLinkedHashMap.PersistentLinkedHashMapMutable
import jetbrains.exodus.core.dataStructures.persistent.PersistentLinkedHashMap.RemoveEldestFunction
import java.util.concurrent.atomic.AtomicReference

open class PersistentObjectCache<K, V> : CacheHitRateable {
    private val size: Int
    private val firstGenSizeBound: Int
    private val secondGenSizeBound: Int
    private val root: AtomicReference<Root<K?, V?>?>

    @JvmOverloads
    constructor(size: Int = ObjectCacheBase.Companion.DEFAULT_SIZE, secondGenSizeRatio: Float = 0.5f) {
        var secondGenSizeRatio = secondGenSizeRatio
        this.size = if (size < ObjectCacheBase.Companion.MIN_SIZE) ObjectCacheBase.Companion.MIN_SIZE else size
        if (secondGenSizeRatio < 0.05f) {
            secondGenSizeRatio = 0.05f
        } else if (secondGenSizeRatio > 0.95f) {
            secondGenSizeRatio = 0.95f
        }
        secondGenSizeBound = (size * secondGenSizeRatio).toInt()
        firstGenSizeBound = size - secondGenSizeBound
        root = AtomicReference()
    }

    protected constructor(source: PersistentObjectCache<K?, V?>, listener: EvictListener<K?, V?>?) {
        size = source.size
        firstGenSizeBound = source.firstGenSizeBound
        secondGenSizeBound = source.secondGenSizeBound
        root = AtomicReference(Root.getClone(source.root.get(), listener, firstGenSizeBound, secondGenSizeBound))
        attempts = source.attempts
        hits = source.hits
    }

    fun clear() {
        root.set(null)
    }

    fun size(): Int {
        return size
    }

    fun count(): Int {
        val root = current
        return if (root == null) 0 else root.getFirstGen().size() + root.getSecondGen().size()
    }

    operator fun get(key: K): V? {
        return tryKey(key)
    }

    fun put(key: K, x: V) {
        cacheObject(key, x)
    }

    fun tryKey(key: K): V? {
        incAttempts()
        var current: Root<K?, V?>?
        var next: Root<K?, V?>
        var result: V
        do {
            current = this.current
            next = Root(current, firstGenSizeBound, secondGenSizeBound)
            val secondGen: PersistentLinkedHashMap<K?, V?> = next.getSecondGen()
            val secondGenMutable = secondGen.beginWrite()
            result = secondGenMutable!![key]
            var wereMutations = secondGenMutable.isDirty
            if (result == null) {
                val firstGen: PersistentLinkedHashMap<K?, V?> = next.getFirstGen()
                val firstGenMutable = firstGen.beginWrite()
                result = firstGenMutable!![key]
                if (!firstGenMutable.isDirty && firstGenMutable.size() >= firstGenSizeBound shr 1) {
                    // if result is not null then move it to the second generation
                    if (result != null) {
                        firstGenMutable.remove(key)
                        secondGenMutable.put(key, result)
                    }
                }
                if (firstGenMutable.isDirty) {
                    wereMutations = true
                    if (!firstGen.endWrite(firstGenMutable)) {
                        PersistentLinkedHashMap.Companion.logMapIsInconsistent()
                    }
                }
            }
            if (!wereMutations) {
                break
            }
            if (secondGenMutable.isDirty && !secondGen.endWrite(secondGenMutable)) {
                PersistentLinkedHashMap.Companion.logMapIsInconsistent()
            }
        } while (!root.compareAndSet(current, next))
        if (result != null) {
            incHits()
        }
        return result
    }

    fun getObject(key: K): V? {
        val current = current ?: return null
        var result: V = current.getFirstGen().beginWrite().getValue(key)
        if (result == null) {
            result = current.getSecondGen().beginWrite().getValue(key)
        }
        return result
    }

    fun cacheObject(key: K, x: V) {
        var current: Root<K?, V?>?
        var next: Root<K?, V?>
        do {
            current = this.current
            next = Root(current, firstGenSizeBound, secondGenSizeBound)
            val firstGen: PersistentLinkedHashMap<K?, V?> = next.getFirstGen()
            val firstGenMutable = firstGen.beginWrite()
            val secondGen: PersistentLinkedHashMap<K?, V?> = next.getSecondGen()
            val secondGenMutable = secondGen.beginWrite()
            if (firstGenMutable!!.remove(key) == null) {
                secondGenMutable!!.remove(key)
            }
            if (secondGenMutable!!.size() < secondGenSizeBound shr 1) {
                secondGenMutable.put(key, x)
            } else {
                firstGenMutable.put(key, x)
            }
            if (firstGenMutable.isDirty && !firstGen.endWrite(firstGenMutable)) {
                PersistentLinkedHashMap.Companion.logMapIsInconsistent()
            }
            if (secondGenMutable.isDirty && !secondGen.endWrite(secondGenMutable)) {
                PersistentLinkedHashMap.Companion.logMapIsInconsistent()
            }
        } while (!root.compareAndSet(current, next))
    }

    fun remove(key: K): V? {
        var current: Root<K?, V?>?
        var next: Root<K?, V?>
        var result: V
        do {
            current = this.current
            next = Root(current, firstGenSizeBound, secondGenSizeBound)
            val firstGen: PersistentLinkedHashMap<K?, V?> = next.getFirstGen()
            val firstGenMutable = firstGen.beginWrite()
            result = firstGenMutable!!.remove(key)
            if (result != null) {
                if (!firstGen.endWrite(firstGenMutable)) {
                    PersistentLinkedHashMap.Companion.logMapIsInconsistent()
                }
            } else {
                val secondGen: PersistentLinkedHashMap<K?, V?> = next.getSecondGen()
                val secondGenMutable = secondGen.beginWrite()
                result = secondGenMutable!!.remove(key)
                if (result == null) {
                    break
                }
                if (!secondGen.endWrite(secondGenMutable)) {
                    PersistentLinkedHashMap.Companion.logMapIsInconsistent()
                }
            }
        } while (!root.compareAndSet(current, next))
        return result
    }

    fun forEachKey(procedure: ObjectProcedure<K?>?) {
        val current = current ?: return
        current.getFirstGen().beginWrite().forEachKey(procedure)
        current.getSecondGen().beginWrite().forEachKey(procedure)
    }

    fun forEachEntry(procedure: PairProcedure<K?, V?>?) {
        val current = current ?: return
        current.getFirstGen().beginWrite().forEachEntry(procedure)
        current.getSecondGen().beginWrite().forEachEntry(procedure)
    }

    fun keys(): Iterator<K> {
        val current = current ?: return ArrayList<K>(1).iterator()
        return object : MutableIterator<K> {
            private var firstGenIt: Iterator<Pair<K?, V?>?>? = current.getFirstGen().beginWrite().iterator()
            private var secondGenIt: Iterator<Pair<K?, V?>?>? = null
            private var next: K? = null
            override fun hasNext(): Boolean {
                checkNext()
                return next != null
            }

            override fun next(): K {
                checkNext()
                val result = next
                next = null
                return result
            }

            override fun remove() {
                throw UnsupportedOperationException("PersistentObjectCache.keys iterator is immutable")
            }

            private fun checkNext() {
                if (next == null) {
                    if (firstGenIt != null) {
                        if (firstGenIt!!.hasNext()) {
                            next = firstGenIt!!.next().getFirst()
                            return
                        }
                        firstGenIt = null
                        secondGenIt = current.getSecondGen().beginWrite().iterator()
                    }
                    if (secondGenIt != null) {
                        if (secondGenIt!!.hasNext()) {
                            next = secondGenIt!!.next().getFirst()
                            return
                        }
                        secondGenIt = null
                    }
                }
            }
        }
    }

    fun values(): Iterator<V?> {
        val current = current ?: return ArrayList<V>(1).iterator()
        val result = ArrayList<V?>()
        for (pair in current.getFirstGen().beginWrite()) {
            result.add(pair.getSecond())
        }
        for (pair in current.getSecondGen().beginWrite()) {
            result.add(pair.getSecond())
        }
        return result.iterator()
    }

    open fun getClone(listener: EvictListener<K, V>?): PersistentObjectCache<K, V>? {
        return PersistentObjectCache<K, V>(this, listener)
    }

    private val current: Root<K?, V?>?
        private get() = root.get()

    private class Root<K, V> {
        val firstGen: PersistentLinkedHashMap<K?, V?>
        val secondGen: PersistentLinkedHashMap<K?, V?>

        constructor(sourceRoot: Root<K, V>?, firstGenSizeBound: Int, secondGenSizeBound: Int) {
            if (sourceRoot != null) {
                firstGen = sourceRoot.firstGen.clone
                secondGen = sourceRoot.secondGen.clone
            } else {
                firstGen =
                    PersistentLinkedHashMap(RemoveEldestFunction { map: PersistentLinkedHashMapMutable<K, V>, key: K, value: V -> map.size() > firstGenSizeBound })
                secondGen =
                    PersistentLinkedHashMap(RemoveEldestFunction { map: PersistentLinkedHashMapMutable<K, V>, key: K, value: V -> map.size() > secondGenSizeBound })
            }
        }

        constructor(firstGen: PersistentLinkedHashMap<K, V>, secondGen: PersistentLinkedHashMap<K, V>) {
            this.firstGen = firstGen
            this.secondGen = secondGen
        }

        companion object {
            fun <K, V> getClone(
                sourceRoot: Root<K, V>?,
                listener: EvictListener<K, V>?,
                firstGenSizeBound: Int, secondGenSizeBound: Int
            ): Root<K?, V?> {
                if (listener == null) {
                    val firstGenEvict: RemoveEldestFunction<K?, V?> =
                        RemoveEldestFunction { map: PersistentLinkedHashMapMutable<K, V>, key: K, value: V -> map.size() > firstGenSizeBound }
                    val secondGenEvict: RemoveEldestFunction<K?, V?> =
                        RemoveEldestFunction { map: PersistentLinkedHashMapMutable<K, V>, key: K, value: V -> map.size() > secondGenSizeBound }
                    return if (sourceRoot != null) {
                        Root(sourceRoot.firstGen.getClone(firstGenEvict), sourceRoot.secondGen.getClone(secondGenEvict))
                    } else {
                        Root(PersistentLinkedHashMap(firstGenEvict), PersistentLinkedHashMap(secondGenEvict))
                    }
                }
                val firstGenEvict: RemoveEldestFunction<K?, V?> =
                    RemoveEldestFunction { map: PersistentLinkedHashMapMutable<K, V>, key: K, value: V ->
                        if (map.size() > firstGenSizeBound) {
                            listener.onEvict(key, value)
                            return@RemoveEldestFunction true
                        }
                        false
                    }
                val secondGenEvict: RemoveEldestFunction<K?, V?> =
                    RemoveEldestFunction { map: PersistentLinkedHashMapMutable<K, V>, key: K, value: V ->
                        if (map.size() > secondGenSizeBound) {
                            listener.onEvict(key, value)
                            return@RemoveEldestFunction true
                        }
                        false
                    }
                return if (sourceRoot != null) {
                    Root(sourceRoot.firstGen.getClone(firstGenEvict), sourceRoot.secondGen.getClone(secondGenEvict))
                } else {
                    Root(PersistentLinkedHashMap(firstGenEvict), PersistentLinkedHashMap(secondGenEvict))
                }
            }
        }
    }
}
