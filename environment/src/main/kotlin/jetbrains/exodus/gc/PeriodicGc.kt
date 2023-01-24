/**
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
package jetbrains.exodus.gc

import jetbrains.exodus.core.execution.SharedTimer
import java.lang.ref.WeakReference

class PeriodicGc(gc: GarbageCollector) : SharedTimer.ExpirablePeriodicTask {

    private val gcRef = WeakReference(gc)
    private val gc: GarbageCollector? get() = gcRef.get()

    override val isExpired: Boolean get() = gc == null

    override fun run() {
        gc?.let { gc ->
            val env = gc.environment
            val period = env.environmentConfig.gcRunEvery
            if (period > 0 && gc.lastInvocationTime + (period.toLong() * 1000L) < System.currentTimeMillis()) {
                env.gc()
            }
        }
    }
}