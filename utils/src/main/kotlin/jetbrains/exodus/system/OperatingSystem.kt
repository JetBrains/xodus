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