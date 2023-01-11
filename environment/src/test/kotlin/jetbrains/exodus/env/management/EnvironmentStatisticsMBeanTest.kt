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
package jetbrains.exodus.env.management

import jetbrains.exodus.env.EnvironmentTestsBase
import org.junit.Assert
import org.junit.Test
import java.lang.management.ManagementFactory
import javax.management.ObjectName

class EnvironmentStatisticsMBeanTest : EnvironmentTestsBase() {

    @Test
    fun beanIsAccessible() {
        val envConfigInstances = platformMBeanServer.queryMBeans(ObjectName(EnvironmentStatistics.getObjectName(env)), null)
        Assert.assertNotNull(envConfigInstances)
        Assert.assertFalse(envConfigInstances.isEmpty())
    }

    companion object {
        private val platformMBeanServer = ManagementFactory.getPlatformMBeanServer()
    }
}

