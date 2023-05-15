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

import jetbrains.exodus.core.dataStructures.hash.LinkedHashMap
import java.util.*
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

open class ObjectCache<K, V> @JvmOverloads constructor(
    cacheSize: Int = ObjectCacheBase.Companion.DEFAULT_SIZE,
    secondGenSizeRatio: Float = DEFAULT_SECOND_GENERATION_QUEUE_SIZE_RATIO
) : ObjectCacheBase<K, V>(cacheSize) {
    private val lock: Lock?
    private val secondGenSizeRatio: Float
    private var firstGenerationQueue: LinkedHashMap<K?, V?>? = null
    private var secondGenerationQueue: LinkedHashMap<K?, V?>? = null
    private var listeners: Array<DeletedPairsListener<K?, V?>>?
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
        addDeletedPairsListener(DeletedPairsListener<K, V> { key: K, value: V -> pushedOutValue = value })
    }

    override fun clear() {
        if (firstGenerationQueue != null && secondGenerationQueue != null && isEmpty) {
            return
        }
        firstGenerationQueue = object : LinkedHashMap<K, V>() {
            override fun removeEldestEntry(eldest: Map.Entry<K, V>?): Boolean {
                val result = size + secondGenerationQueue!!.size > this@ObjectCache.size
                if (result) {
                    fireListenersAboutDeletion(eldest!!.key, eldest.value)
                }
                return result
            }
        }
        val secondGenSizeBound = (size * secondGenSizeRatio).toInt()
        secondGenerationQueue = object : LinkedHashMap<K, V>() {
            override fun removeEldestEntry(eldest: Map.Entry<K, V>?): Boolean {
                val result = size > secondGenSizeBound
                if (result) {
                    --internalSize
                    firstGenerationQueue[eldest!!.key] = eldest.value
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

    override fun remove(key: K): V {
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
        for ((key, value) in firstGenerationQueue!!) {
            fireListenersAboutDeletion(key, value)
        }
        for ((key, value) in secondGenerationQueue!!) {
            fireListenersAboutDeletion(key, value)
        }
        clear()
    }

    // returns value pushed out of the cache
    override fun cacheObject(key: K, x: V): V {
        pushedOutValue = null
        if (firstGenerationQueue.put(key, x) == null) {
            secondGenerationQueue!!.remove(key)
        }
        return pushedOutValue
    }

    override fun tryKey(key: K): V {
        incAttempts()
        var result = secondGenerationQueue!![key]
        if (result == null) {
            result = firstGenerationQueue!!.remove(key)
            if (result != null) {
                secondGenerationQueue[key] = result
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
    override fun getObject(key: K): V {
        var result = firstGenerationQueue!![key]
        if (result == null) {
            result = secondGenerationQueue!![key]
        }
        return result
    }

    override fun count(): Int {
        return firstGenerationQueue!!.size + secondGenerationQueue!!.size
    }

    fun keys(): Iterator<K> {
        return ObjectCacheKeysIterator<K, V>(this)
    }

    fun values(): Iterator<V> {
        return ObjectCacheValuesIterator(this)
    }

    protected class ObjectCacheKeysIterator<K, V>(cache: ObjectCache<K?, V>) : MutableIterator<K?> {
        private val firstGenIterator: MutableIterator<K?>
        private val secondGenIterator: MutableIterator<K?>

        init {
            firstGenIterator = cache.firstGenerationQueue!!.keys.iterator()
            secondGenIterator = cache.secondGenerationQueue!!.keys.iterator()
        }

        override fun hasNext(): Boolean {
            return firstGenIterator.hasNext() || secondGenIterator.hasNext()
        }

        override fun next(): K? {
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

    protected class ObjectCacheValuesIterator<K, V>(cache: ObjectCache<K, V?>) : MutableIterator<V?> {
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
    interface DeletedPairsListener<K, V> : EventListener {
        fun objectRemoved(key: K, value: V)
    }

    fun addDeletedPairsListener(listener: DeletedPairsListener<K?, V?>) {
        if (listeners == null) {
            listeners = arrayOfNulls<DeletedPairsListener<*, *>>(1)
        } else {
            val newListeners: Array<DeletedPairsListener<K?, V?>> = arrayOfNulls<DeletedPairsListener<*, *>>(
                listeners!!.size + 1
            )
            System.arraycopy(listeners, 0, newListeners, 0, listeners!!.size)
            listeners = newListeners
        }
        listeners!![listeners!!.size - 1] = listener
    }

    fun removeDeletedPairsListener(listener: DeletedPairsListener<K, V>) {
        if (listeners != null) {
            if (listeners!!.size == 1) {
                listeners = null
            } else {
                val newListeners: Array<DeletedPairsListener<K?, V?>> = arrayOfNulls<DeletedPairsListener<*, *>>(
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

    private fun fireListenersAboutDeletion(key: K?, x: V?) {
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
