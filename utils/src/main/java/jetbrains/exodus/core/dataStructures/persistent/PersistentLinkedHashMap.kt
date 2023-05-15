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

import jetbrains.exodus.core.dataStructures.LongObjectCacheBase.size
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.hash.*
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashMap.MutablePersistentHashMap
import org.slf4j.LoggerFactory

class PersistentLinkedHashMap<K, V> {
    @Volatile
    private var root: Root<K, V>? = null
    private val removeEldest: RemoveEldestFunction<K, V>?

    constructor() {
        root = null
        removeEldest = null
    }

    constructor(removeEldest: RemoveEldestFunction<K, V>?) {
        root = null
        this.removeEldest = removeEldest
    }

    private constructor(source: PersistentLinkedHashMap<K, V>, removeEldest: RemoveEldestFunction<K, V>?) {
        val sourceRoot = source.root
        root = if (sourceRoot == null) {
            Root()
        } else {
            Root(sourceRoot)
        }
        this.removeEldest = removeEldest
    }

    fun size(): Int {
        val root: Root<K?, V>? = this.root
        return root?.map?.current?.size() ?: 0
    }

    val isEmpty: Boolean
        get() = size() == 0
    val clone: PersistentLinkedHashMap<K, V>
        get() = PersistentLinkedHashMap(this, removeEldest)

    fun getClone(removeEldestFunction: RemoveEldestFunction<K, V>?): PersistentLinkedHashMap<K, V> {
        return PersistentLinkedHashMap(this, removeEldestFunction)
    }

    fun beginWrite(): PersistentLinkedHashMapMutable<K, V> {
        return PersistentLinkedHashMapMutable(this)
    }

    fun endWrite(mutableMap: PersistentLinkedHashMapMutable<K, V>): Boolean {
        if (!mutableMap.isDirty || root != null && mutableMap.root !== root) {
            return false
        }
        // TODO: this is a relaxed condition (not to break existing behaviour)
        val result = mutableMap.endWrite()
        root = Root<K, V>(mutableMap.root.map, mutableMap.root.queue, mutableMap.order)
        return result
    }

    class PersistentLinkedHashMapMutable<K, V>(source: PersistentLinkedHashMap<K?, V?>) : Iterable<Pair<K, V>?> {
        private var root: Root<K?, V?> = null
        var order: Long = 0
        private val removeEldest: RemoveEldestFunction<K?, V?>?
        private val mapMutable: MutablePersistentHashMap
        private val queueMutable: PersistentLongMap.MutableMap<K?>
        var isDirty: Boolean
            private set

        init {
            val sourceRoot = source.root
            if (sourceRoot == null) {
                root = Root()
                order = 0L
            } else {
                root = sourceRoot
                order = sourceRoot.order
            }
            removeEldest = source.removeEldest
            mapMutable = root.map.beginWrite()
            queueMutable = root.queue.beginWrite()
            isDirty = false
        }

        operator fun get(key: K): V? {
            val internalValue: InternalValue<V> = mapMutable.get(key) ?: return null
            val result: V = internalValue.getValue()
            val currentOrder: Long = internalValue.getOrder()
            if (root.order > currentOrder + (mapMutable.size() shr 1)) {
                isDirty = true
                val newOrder = ++order
                mapMutable.put(key, InternalValue(newOrder, result))
                queueMutable.put(newOrder, key)
                removeKeyAndCheckConsistency(key, currentOrder)
            }
            return result
        }

        fun getValue(key: K): V? {
            val internalValue: InternalValue<V> = mapMutable.get(key)
            return if (internalValue == null) null else internalValue.getValue()
        }

        fun containsKey(key: K): Boolean {
            return mapMutable.get(key) != null
        }

        fun put(key: K, value: V?) {
            val internalValue: InternalValue<V> = mapMutable.get(key)
            if (internalValue != null) {
                removeKeyAndCheckConsistency(key, internalValue.getOrder())
            }
            isDirty = true
            val newOrder = ++order
            mapMutable.put(key, InternalValue(newOrder, value))
            queueMutable.put(newOrder, key)
            if (removeEldest != null) {
                var removed = 0
                var min: PersistentLongMap.Entry<K>
                while (queueMutable.minimum.also { min = it } != null) {
                    if (removed >= 50) {
                        break // prevent looping on implementation errors
                    }
                    val eldestKey = min.value
                    if (removeEldest.removeEldest(this, eldestKey, getValue(eldestKey))) {
                        isDirty = true
                        mapMutable.removeKey(eldestKey) // removeKey may do nothing, but we still must
                        queueMutable.remove(min.key) // remove min key from the order queue
                        removed++
                    } else {
                        break
                    }
                }
                if (removed >= 35 && logger.isWarnEnabled) {
                    logger.warn("PersistentLinkedHashMap evicted $removed keys during a single put().", Throwable())
                }
            }
        }

        fun remove(key: K): V? {
            val internalValue: InternalValue<V> = mapMutable.removeKey(key)
            if (internalValue != null) {
                isDirty = true
                removeKeyAndCheckConsistency(key, internalValue.getOrder())
                return internalValue.getValue()
            }
            return null
        }

        fun size(): Int {
            return mapMutable.size()
        }

        val isEmpty: Boolean
            get() = size() == 0

        fun forEachKey(procedure: ObjectProcedure<K>) {
            mapMutable.forEachKey(ObjectProcedure<PersistentHashMap.Entry<K, InternalValue<V>>> { `object`: PersistentHashMap.Entry<K, InternalValue<V>?> ->
                procedure.execute(
                    `object`.key
                )
            })
        }

        fun forEachEntry(procedure: PairProcedure<K, V>) {
            mapMutable.forEachKey(ObjectProcedure { `object`: PersistentHashMap.Entry<K, InternalValue<V>> ->
                procedure.execute(
                    `object`.key,
                    `object`.value.getValue()
                )
            })
        }

        override fun iterator(): MutableIterator<Pair<K, V>> {
            val sourceIt: MutableIterator<PersistentHashMap.Entry<K, InternalValue<V>>> = mapMutable.iterator()
            return object : MutableIterator<Pair<K, V>?> {
                override fun hasNext(): Boolean {
                    return sourceIt.hasNext()
                }

                override fun next(): Pair<K, V> {
                    val next = sourceIt.next()
                    return Pair(next.key, next.value.getValue())
                }

                override fun remove() {
                    sourceIt.remove()
                }
            }
        }

        fun endWrite(): Boolean {
            return mapMutable.endWrite() && queueMutable.endWrite()
        }

        fun checkTip() {
            mapMutable.checkTip()
            queueMutable.testConsistency()
        }

        private fun removeKeyAndCheckConsistency(key: K, prevOrder: Long) {
            val keyByOrder = queueMutable.remove(prevOrder)
            if (key != keyByOrder) {
                logger.error(
                    "PersistentLinkedHashMap is inconsistent, key = " + key + ", keyByOrder = " + keyByOrder +
                            ", prevOrder = " + prevOrder, Throwable()
                )
            }
        }
    }

    interface RemoveEldestFunction<K, V> {
        fun removeEldest(
            map: PersistentLinkedHashMapMutable<K, V>,
            key: K,
            value: V?
        ): Boolean
    }

    private class Root<K, V>(
        map: PersistentHashMap<K?, InternalValue<V>> = PersistentHashMap(),
        queue: PersistentLongMap<K?> = PersistentLong23TreeMap(), order: Long = 0L
    ) {
        val map: PersistentHashMap<K, InternalValue<V>>
        val queue: PersistentLongMap<K>
        val order: Long

        constructor(source: Root<K, V>) : this(source.map.clone, source.queue.clone, source.order)

        init {
            this.map = map
            this.queue = queue
            this.order = order
        }
    }

    private class InternalValue<V> internal constructor(val order: Long, val value: V)
    companion object {
        private val logger = LoggerFactory.getLogger(PersistentLinkedHashMap::class.java)

        /**
         * Logs the error that the map is inconsistent instead of throwing an exception. This leaves the chance for
         * the map to recovery to a consistent state.
         */
        fun logMapIsInconsistent() {
            logger.error("PersistentLinkedHashMap is inconsistent", Throwable())
        }
    }
}
