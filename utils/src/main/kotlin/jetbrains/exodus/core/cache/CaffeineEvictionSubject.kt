/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.core.cache

import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import java.util.*
import java.util.Collections.synchronizedMap

interface EvictionListener<K, V> {
    fun onEvict(key: K?, value: V?)
}

internal class CaffeineEvictionSubject<K, V> : RemovalListener<K, V> {

    private val listeners: MutableMap<EvictionListener<K, V>, Any> = synchronizedMap(WeakHashMap())

    override fun onRemoval(key: K?, value: V?, cause: RemovalCause?) {
        val keys = listeners.keys
        synchronized(listeners) {
            keys.forEach { it.onEvict(key, value) }
        }
    }

    fun addListener(listener: EvictionListener<K, V>) {
        listeners[listener] = listener
    }
}