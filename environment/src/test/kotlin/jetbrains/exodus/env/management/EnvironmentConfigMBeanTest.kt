/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.env.management

import jetbrains.exodus.TestUtil
import jetbrains.exodus.env.EnvironmentTestsBase
import jetbrains.exodus.env.ReadonlyTransactionException
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.junit.Assert
import org.junit.Test
import java.lang.management.ManagementFactory
import javax.management.Attribute
import javax.management.ObjectName

class EnvironmentConfigMBeanTest : EnvironmentTestsBase() {

    private var envConfigName: ObjectName? = null

    override fun setUp() {
        super.setUp()
        envConfigName = null
        envConfigName = ObjectName(EnvironmentConfig.getObjectName(env))
    }

    @Test
    fun beanIsAccessible() {
        Assert.assertNotNull(envConfigName)
        val envConfigInstances = platformMBeanServer.queryMBeans(ObjectName(EnvironmentConfig.getObjectName(env)), null)
        Assert.assertNotNull(envConfigInstances)
        Assert.assertFalse(envConfigInstances.isEmpty())
    }

    @Test
    fun readOnly() {
        beanIsAccessible()
        Assert.assertFalse((platformMBeanServer.getAttribute(envConfigName, READ_ONLY_ATTR) as Boolean))
        platformMBeanServer.setAttribute(envConfigName, Attribute(READ_ONLY_ATTR, true))
        Assert.assertTrue((platformMBeanServer.getAttribute(envConfigName, READ_ONLY_ATTR) as Boolean))
        Assert.assertTrue(env.environmentConfig.envIsReadonly)
        platformMBeanServer.setAttribute(envConfigName, Attribute(READ_ONLY_ATTR, false))
    }

    @Test
    fun readOnly_XD_444() {
        beanIsAccessible()
        val txn: Transaction = env.beginTransaction()
        try {
            env.openStore("New Store", StoreConfig.WITHOUT_DUPLICATES, txn)
            Assert.assertFalse(txn.isIdempotent)
            platformMBeanServer.setAttribute(envConfigName, Attribute(READ_ONLY_ATTR, true))
            TestUtil.runWithExpectedException({ txn.flush() }, ReadonlyTransactionException::class.java)
        } finally {
            txn.abort()
        }
    }

    @Test
    fun readOnly_XD_448() {
        beanIsAccessible()
        platformMBeanServer.setAttribute(envConfigName, Attribute(READ_ONLY_ATTR, true))
        platformMBeanServer.setAttribute(envConfigName, Attribute(READ_ONLY_ATTR, true))
    }

    companion object {
        private val platformMBeanServer = ManagementFactory.getPlatformMBeanServer()
        private const val READ_ONLY_ATTR = "EnvIsReadonly"
    }
}
