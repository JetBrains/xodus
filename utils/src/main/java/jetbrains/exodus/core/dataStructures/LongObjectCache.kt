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
package jetbrains.exodus.core.dataStructures

import jetbrains.exodus.core.dataStructures.hash.*
import java.util.*
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

open class LongObjectCache<V> @JvmOverloads constructor(
    cacheSize: Int = DEFAULT_SIZE,
    secondGenSizeRatio: Float = DEFAULT_SECOND_GENERATION_QUEUE_SIZE_RATIO
) : LongObjectCacheBase<V>(cacheSize) {
    private val lock: Lock?
    private val secondGenSizeRatio: Float
    private var firstGenerationQueue: LongLinkedHashMap<V?>? = null
    private var secondGenerationQueue: LongLinkedHashMap<V>? = null
    private var listeners: Array<DeletedPairsListener<V?>>?
    private var pushedOutValue: V? = null

    init {
        var secondGenSizeRatio = secondGenSizeRatio
        lock = ReentrantLock()
        if (secondGenSizeRatio < 0.05f) {
            secondGenSizeRatio = 0.05f
        } else if (secondGenSizeRatio > 0.95f) {
            secondGenSizeRatio = 0.95f
        }
        this.secondGenSizeRatio = secondGenSizeRatio
        clear()
        addDeletedPairsListener(DeletedPairsListener<V> { key: Long, value: V -> pushedOutValue = value })
    }

    override fun clear() {
        firstGenerationQueue = object : LongLinkedHashMap<V>() {
            override fun removeEldestEntry(eldest: Map.Entry<Long, V>?): Boolean {
                val result = size + secondGenerationQueue!!.size > size()
                if (result) {
                    fireListenersAboutDeletion(eldest!!.key, eldest.value)
                }
                return result
            }
        }
        val secondGenSizeBound = (size() * secondGenSizeRatio).toInt()
        secondGenerationQueue = object : LongLinkedHashMap<V>() {
            override fun removeEldestEntry(eldest: Map.Entry<Long, V>?): Boolean {
                val result = size > secondGenSizeBound
                if (result) {
                    --internalSize
                    firstGenerationQueue.put(eldest!!.key, eldest.value)
                    ++internalSize
                }
                return result
            }
        }
    }

    override fun lock() {
        lock?.lock()
    }

    override fun unlock() {
        lock?.unlock()
    }

    // returns value pushed out of the cache
    override fun remove(key: Long): V? {
        var x = firstGenerationQueue!!.remove(key)
        if (x != null) {
            fireListenersAboutDeletion(key, x)
        } else {
            x = secondGenerationQueue!!.remove(key)
            x?.let { fireListenersAboutDeletion(key, it) }
        }
        return x
    }

    fun removeAll() {
        for (key in firstGenerationQueue!!.keys) {
            fireListenersAboutDeletion(key, firstGenerationQueue!![key])
        }
        for (key in secondGenerationQueue!!.keys) {
            fireListenersAboutDeletion(key, secondGenerationQueue!![key])
        }
        clear()
    }

    // returns value pushed out of the cache
    override fun cacheObject(key: Long, x: V): V? {
        pushedOutValue = null
        if (firstGenerationQueue.put(key, x) == null) {
            secondGenerationQueue!!.remove(key)
        }
        return pushedOutValue
    }

    override fun tryKey(key: Long): V? {
        incAttempts()
        var result = secondGenerationQueue!![key]
        if (result == null) {
            result = firstGenerationQueue!!.remove(key)
            if (result != null) {
                secondGenerationQueue.put(key, result)
            }
        }
        if (result != null) {
            incHits()
        }
        return result
    }

    /**
     * @param key key.
     * @return object from the cache not affecting usages statistics.
     */
    override fun getObject(key: Long): V? {
        var result = firstGenerationQueue!![key]
        if (result == null) {
            result = secondGenerationQueue!![key]
        }
        return result
    }

    override fun count(): Int {
        return firstGenerationQueue!!.size + secondGenerationQueue!!.size
    }

    fun keys(): Iterator<Long> {
        return LongObjectCacheKeysIterator(this)
    }

    fun values(): Iterator<V> {
        return LongObjectCacheValuesIterator(this)
    }

    fun forEachEntry(procedure: ObjectProcedure<Map.Entry<Long?, V?>?>): Boolean {
        for (entry in firstGenerationQueue!!.entries) {
            if (!procedure.execute(entry)) return false
        }
        for (entry in secondGenerationQueue!!.entries) {
            if (!procedure.execute(entry)) return false
        }
        return true
    }

    protected class LongObjectCacheKeysIterator<V>(cache: LongObjectCache<V>) : MutableIterator<Long> {
        private val firstGenIterator: MutableIterator<Long>
        private val secondGenIterator: MutableIterator<Long>

        init {
            firstGenIterator = cache.firstGenerationQueue!!.keys.iterator()
            secondGenIterator = cache.secondGenerationQueue!!.keys.iterator()
        }

        override fun hasNext(): Boolean {
            return firstGenIterator.hasNext() || secondGenIterator.hasNext()
        }

        override fun next(): Long {
            return if (firstGenIterator.hasNext()) firstGenIterator.next() else secondGenIterator.next()
        }

        override fun remove() {
            if (firstGenIterator.hasNext()) {
                firstGenIterator.remove()
            } else {
                secondGenIterator.remove()
            }
        }
    }

    protected class LongObjectCacheValuesIterator<V>(cache: LongObjectCache<V?>) : MutableIterator<V?> {
        private val firstGenIterator: MutableIterator<V?>
        private val secondGenIterator: MutableIterator<V?>

        init {
            firstGenIterator = cache.firstGenerationQueue!!.values.iterator()
            secondGenIterator = cache.secondGenerationQueue!!.values.iterator()
        }

        override fun hasNext(): Boolean {
            return firstGenIterator.hasNext() || secondGenIterator.hasNext()
        }

        override fun next(): V? {
            return if (firstGenIterator.hasNext()) firstGenIterator.next() else secondGenIterator.next()
        }

        override fun remove() {
            if (firstGenIterator.hasNext()) {
                firstGenIterator.remove()
            } else {
                secondGenIterator.remove()
            }
        }
    }

    // start of listening features
    interface DeletedPairsListener<V> : EventListener {
        fun objectRemoved(key: Long, value: V)
    }

    fun addDeletedPairsListener(listener: DeletedPairsListener<V?>) {
        if (listeners == null) {
            listeners = arrayOfNulls<DeletedPairsListener<*>>(1)
        } else {
            val newListeners: Array<DeletedPairsListener<V?>> = arrayOfNulls<DeletedPairsListener<*>>(
                listeners!!.size + 1
            )
            System.arraycopy(listeners, 0, newListeners, 0, listeners!!.size)
            listeners = newListeners
        }
        listeners!![listeners!!.size - 1] = listener
    }

    fun removeDeletedPairsListener(listener: DeletedPairsListener<V>) {
        if (listeners != null) {
            if (listeners!!.size == 1) {
                listeners = null
            } else {
                val newListeners: Array<DeletedPairsListener<V?>> = arrayOfNulls<DeletedPairsListener<*>>(
                    listeners!!.size - 1
                )
                var i = 0
                for (myListener in listeners!!) {
                    if (myListener !== listener) {
                        newListeners[i++] = myListener
                    }
                }
                listeners = newListeners
            }
        }
    }

    private fun fireListenersAboutDeletion(key: Long, x: V?) {
        if (listeners != null) {
            for (myListener in listeners!!) {
                myListener.objectRemoved(key, x)
            }
        }
    } // end of listening features

    companion object {
        const val DEFAULT_SECOND_GENERATION_QUEUE_SIZE_RATIO = 0.4f
    }
}
