/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
import java.lang.management.ManagementFactory
import java.lang.ref.WeakReference

object OperatingSystem {

    private val osBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    private var cachedPhysicalMemorySize = WeakReference<Long>(null)
    private var cachedSystemCpuLoad = WeakReference<Double>(null)

    fun getFreePhysicalMemorySize(): Long {
        var result = cachedPhysicalMemorySize.get()
        if (result == null) {
            result = osBean.freePhysicalMemorySize
            cachedPhysicalMemorySize = WeakReference(result)
        }
        return result
    }

    fun getSystemCpuLoad(): Double {
        var result = cachedSystemCpuLoad.get()
        if (result == null) {
            result = osBean.systemCpuLoad
            cachedSystemCpuLoad = WeakReference(result)
        }
        return result
    }
}