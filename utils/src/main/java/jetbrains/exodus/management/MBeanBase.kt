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
package jetbrains.exodus.management

import java.lang.management.ManagementFactory
import javax.management.InstanceNotFoundException
import javax.management.ObjectName

abstract class MBeanBase protected constructor(objectName: String) {
    var name: ObjectName = null
    private var runOnClose: Runnable? = null

    init {
        try {
            name = ObjectName(objectName)
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, name)
        } catch (e: Exception) {
            throw if (e is RuntimeException) e else RuntimeException(e)
        }
    }

    open fun close() {
        val runOnClose = runOnClose
        runOnClose?.run()
    }

    fun unregister() {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name)
        } catch (ignore: InstanceNotFoundException) {
        } catch (e: Exception) {
            throw if (e is RuntimeException) e else RuntimeException(e)
        }
    }

    fun runOnClose(runnable: Runnable?) {
        runOnClose = runnable
    }

    companion object {
        @JvmStatic
        fun escapeLocation(location: String): String {
            return if (location.indexOf(':') >= 0) location.replace(':', '@') else location
        }
    }
}
