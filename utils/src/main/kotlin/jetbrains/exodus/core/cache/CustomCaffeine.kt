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

import com.github.benmanes.caffeine.cache.Cache
import java.util.concurrent.atomic.AtomicLong

class CustomCaffeine<K, V>(private val cache: Cache<K, V>) : Cache<K, V> by cache {

    // This is a workaround to get maximum size of the cache quickly without locking
    // as it is implemented in Caffeine as of time being.
    private var sizeRef = AtomicLong(cache.policy().eviction().orElseThrow().maximum)

    fun trySetSize(targetSize: Long): Boolean {
        val currentSize = sizeRef.get()
        if (this.sizeRef.compareAndSet(currentSize, targetSize)) {
            if (currentSize == targetSize) {
                // Size is not actually changed
                return false
            } else {
                cache.policy().eviction().orElseThrow().maximum = targetSize
                return true
            }
        }
        return false
    }

    fun getSize(): Long {
        return sizeRef.get()
    }
}