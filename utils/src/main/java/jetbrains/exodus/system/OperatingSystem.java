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
package jetbrains.exodus.system;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;

public final class OperatingSystem {

    private static final OperatingSystemMXBean osBean;

    static {
        osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    }

    private static WeakReference<Long> cachedPhysicalMemorySize = new WeakReference<>(null);
    private static WeakReference<Double> cachedSystemCpuLoad = new WeakReference<>(null);

    private OperatingSystem() {
    }

    public static long getFreePhysicalMemorySize() {
        Long result = cachedPhysicalMemorySize.get();
        if (result == null) {
            result = osBean.getFreePhysicalMemorySize();
            cachedPhysicalMemorySize = new WeakReference<>(result);
        }
        return result;
    }

    public static double getSystemCpuLoad() {
        Double result = cachedSystemCpuLoad.get();
        if (result == null) {
            result = osBean.getSystemCpuLoad();
            cachedSystemCpuLoad = new WeakReference<>(result);
        }
        return result;
    }
}
