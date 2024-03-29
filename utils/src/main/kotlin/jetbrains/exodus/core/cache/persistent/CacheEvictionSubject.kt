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
package jetbrains.exodus.core.cache.persistent

import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import java.util.concurrent.ConcurrentHashMap

typealias EvictionListener<K> = (key: K?) -> Unit

class CacheEvictionSubject<K> : RemovalListener<K, Any> {

    private val listeners = ConcurrentHashMap.newKeySet<EvictionListener<K>>()

    fun addListener(listener: EvictionListener<K>) {
        listeners.add(listener)
    }

    fun removeListener(listener: EvictionListener<K>) {
        listeners.remove(listener)
    }

    override fun onRemoval(key: K?, value: Any?, cause: RemovalCause?) {
        if (cause == RemovalCause.REPLACED) {
            // Ignore replaced values as key is not actually evicted
            return
        }
        listeners.forEach { it(key) }
    }
}