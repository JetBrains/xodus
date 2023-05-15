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
import jetbrains.exodus.core.execution.SharedTimer.registerPeriodicTask
import jetbrains.exodus.core.execution.SharedTimer.unregisterPeriodicTask
import java.lang.ref.WeakReference

abstract class CacheHitRateable protected constructor() {
    private val cacheAdjuster: ExpirablePeriodicTask?
    open var attempts: Int
    open var hits = 0

    init {
        attempts = hits
        cacheAdjuster = getCacheAdjuster()
        if (cacheAdjuster != null) {
            registerPeriodicTask(cacheAdjuster)
        }
    }

    open fun close() {
        if (cacheAdjuster != null) {
            unregisterPeriodicTask(cacheAdjuster)
        }
    }

    open fun hitRate(): Float {
        val hits = hits
        var attempts = attempts
        // due to lack of thread-safety there can appear not that consistent results
        if (hits > attempts) {
            attempts = hits
        }
        return if (attempts > 0) hits.toFloat() / attempts.toFloat() else 0
    }

    open fun incAttempts() {
        ++attempts
    }

    open fun incHits() {
        ++hits
    }

    open fun adjustHitRate() {
        val hits = hits
        var attempts = attempts
        // due to lack of thread-safety there can appear not that consistent results
        if (hits > attempts) {
            attempts = hits
        }
        if (attempts > 256) {
            this.attempts = attempts + 1 shr 1
            this.hits = hits + 1 shr 1
        }
    }

    protected open fun getCacheAdjuster(): ExpirablePeriodicTask? {
        return CacheAdjuster(this)
    }

    private class CacheAdjuster(cache: CacheHitRateable) : ExpirablePeriodicTask {
        private val cacheRef: WeakReference<CacheHitRateable?>

        init {
            cacheRef = WeakReference(cache)
        }

        override fun isExpired(): Boolean {
            return cacheRef.get() == null
        }

        override fun run() {
            val cache = cacheRef.get()
            cache?.adjustHitRate()
        }
    }
}
