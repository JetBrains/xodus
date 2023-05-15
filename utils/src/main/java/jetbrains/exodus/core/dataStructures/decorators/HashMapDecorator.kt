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
package jetbrains.exodus.core.dataStructures.decorators

class HashMapDecorator<K, V> : MutableMap<K, V> {
    private var decorated: MutableMap<K, V>? = null
    init {
        clear()
    }

    override val size: Int
        get() {
            return decorated?.size ?: 0
        }

    override fun isEmpty(): Boolean {
        val decorated = decorated
        return decorated.isNullOrEmpty()
    }

    override fun containsKey(key: K): Boolean {
        return decorated?.containsKey(key) ?: false
    }

    override fun containsValue(value: V): Boolean {
        return decorated?.containsValue(value) ?: false
    }

    override operator fun get(key: K): V? {
        return decorated?.get(key)
    }

    override fun put(key: K, value: V): V? {
        checkDecorated()
        return decorated!!.put(key, value)
    }

    override fun remove(key: K): V? {
        val decorated = decorated
        if (decorated === null) {
            return null
        }


        val result: V? = decorated.remove(key)
        if (result != null && decorated.isEmpty()) {
            clear()
        }

        return result
    }

    override fun putAll(from: Map<out K, V>) {
        checkDecorated()
        decorated!!.putAll(from)
    }

    override fun clear() {
        decorated = null
    }

    override val keys: MutableSet<K>
        get() {
            checkDecorated()
            return decorated!!.keys
        }

    override val values: MutableCollection<V>
        get() {
            checkDecorated()
            return decorated!!.values
        }

    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() {
            checkDecorated()
            return decorated!!.entries
        }

    private fun checkDecorated() {
        if (decorated == null) {
            decorated = mutableMapOf()
        }
    }
}
