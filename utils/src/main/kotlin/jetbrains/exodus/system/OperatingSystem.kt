/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.system

import com.sun.management.OperatingSystemMXBean
import jetbrains.exodus.core.execution.SharedTimer
import java.lang.management.ManagementFactory

object OperatingSystem {

    private val osBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    private var cachedFreePhysicalMemorySize = Long.MIN_VALUE
    private var cachedSystemCpuLoad = Double.MIN_VALUE
    private val periodicInvalidate by lazy {
        SharedTimer.registerNonExpirablePeriodicTask {
            cachedFreePhysicalMemorySize = Long.MIN_VALUE
            cachedSystemCpuLoad = Double.MIN_VALUE
        }
    }

    @JvmStatic
    fun getFreePhysicalMemorySize(): Long {
        cachedFreePhysicalMemorySize.let { memSize ->
            return if (memSize < 0L) {
                periodicInvalidate
                osBean.freeMemorySize.also { cachedFreePhysicalMemorySize = it }
            } else {
                memSize
            }
        }
    }

    @JvmStatic
    fun getSystemCpuLoad(): Double {
        cachedSystemCpuLoad.let { cpuLoad ->
            return if (cpuLoad < 0) {
                periodicInvalidate
                osBean.cpuLoad.also { cachedSystemCpuLoad = it }
            } else {
                cpuLoad
            }
        }
    }
}