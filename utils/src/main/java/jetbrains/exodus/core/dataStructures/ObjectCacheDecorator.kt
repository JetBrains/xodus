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

import jetbrains.exodus.core.execution.SharedTimer.ExpirablePeriodicTask
import java.util.function.BooleanSupplier

/**
 * Cache decorator for lazy cache creation.
 */
abstract class ObjectCacheDecorator<K, V> @JvmOverloads constructor(
    size: Int = ObjectCacheBase.Companion.DEFAULT_SIZE,
    private val shouldCache: BooleanSupplier = BooleanSupplier { true }
) : ObjectCacheBase<K, V>(size) {
    private var decorated: ObjectCacheBase<K, V>? = null
    override fun clear() {
        if (decorated != null) {
            decorated!!.close()
            decorated = null
        }
    }

    override fun lock() {
        getCache(true).lock()
    }

    override fun unlock() {
        getCache(false).unlock()
    }

    override fun cacheObject(key: K, x: V): V {
        return getCache(true).cacheObject(key, x)
    }

    override fun remove(key: K): V {
        return getCache(false).remove(key)
    }

    override fun tryKey(key: K): V {
        return getCache(false).tryKey(key)
    }

    override fun getObject(key: K): V {
        return getCache(false).getObject(key)
    }

    override fun count(): Int {
        return getCache(false).count()
    }

    override fun close() {
        getCache(false).close()
    }

    override var attempts: Int
        get() = getCache(false).attempts
        set(attempts) {
            super.attempts = attempts
        }
    override var hits: Int
        get() = getCache(false).hits
        set(hits) {
            super.hits = hits
        }

    override fun hitRate(): Float {
        return getCache(false).hitRate()
    }

    override fun newCriticalSection(): CriticalSection? {
        return getCache(true).newCriticalSection()
    }

    override fun incAttempts() {
        getCache(false).incAttempts()
    }

    override fun incHits() {
        getCache(false).incHits()
    }

    override fun getCacheAdjuster(): ExpirablePeriodicTask? {
        return null
    }

    protected abstract fun createdDecorated(): ObjectCacheBase<K, V>?
    private fun getCache(create: Boolean): ObjectCacheBase<K, V> {
        if (decorated == null) {
            if (!create) {
                return FAKE_CACHE
            }
            decorated = createdDecorated()
        }
        return if (shouldCache.asBoolean) decorated!! else FAKE_CACHE
    }

    companion object {
        private val FAKE_CACHE: ObjectCacheBase<*, *> = FakeObjectCache<Any, Any>()
    }
}
