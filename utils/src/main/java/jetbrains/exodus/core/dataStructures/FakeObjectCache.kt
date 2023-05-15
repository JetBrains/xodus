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

class FakeObjectCache<K, V> private constructor(size: Int) : ObjectCacheBase<K, V>(size) {
    constructor() : this(0)

    override fun clear() {
        // do nothing
    }

    override fun lock() {
        // do nothing
    }

    override fun unlock() {
        // do nothing
    }

    override fun cacheObject(key: K, x: V): V {
        return null
    }

    override fun remove(key: K): V {
        return null
    }

    override fun tryKey(key: K): V {
        return null
    }

    override fun getObject(key: K): V {
        return null
    }

    override fun count(): Int {
        return 0
    }

    override var attempts: Int
        get() = 0
        set(attempts) {
            super.attempts = attempts
        }
    override var hits: Int
        get() = 0
        set(hits) {
            super.hits = hits
        }

    override fun hitRate(): Float {
        return 0
    }

    override fun newCriticalSection(): CriticalSection? {
        return ObjectCacheBase.Companion.TRIVIAL_CRITICAL_SECTION
    }

    override fun incAttempts() {
        // do nothing
    }

    override fun incHits() {
        // do nothing
    }

    override fun getCacheAdjuster(): ExpirablePeriodicTask? {
        return null
    }
}
