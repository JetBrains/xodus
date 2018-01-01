/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.env

import jetbrains.exodus.AbstractConfig
import jetbrains.exodus.ConfigSettingChangeListener
import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.kotlin.notNull
import org.junit.Assert
import org.junit.Test

class EnvironmentConfigTest {

    @Test
    fun testCloneDefaults() {
        val defaults = EnvironmentConfig.DEFAULT.settings
        val ec = EnvironmentConfig()
        val stringDefaults = HashMap<String, String>()
        for ((key, value) in defaults) {
            stringDefaults.put(key, value.toString())
        }
        ec.setSettings(stringDefaults)
        for ((key, value) in defaults) {
            Assert.assertEquals(value, ec.getSetting(key))
        }
    }

    @Test
    fun testListenerInvoked() {
        val oldValue = 1L
        val newValue = 2L
        val ec = EnvironmentConfig()
        val callBackMethodsCalled = arrayOfNulls<Boolean>(2)
        ec.memoryUsage = oldValue
        ec.addChangedSettingsListener(object : ConfigSettingChangeListener {
            override fun beforeSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                Assert.assertEquals(EnvironmentConfig.MEMORY_USAGE, key)
                Assert.assertEquals(newValue, value)
                callBackMethodsCalled[0] = true
            }

            override fun afterSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                Assert.assertEquals(EnvironmentConfig.MEMORY_USAGE, key)
                Assert.assertEquals(newValue, value)
                callBackMethodsCalled[1] = true
            }
        })

        ec.memoryUsage = newValue

        Assert.assertArrayEquals(arrayOf(true, true), callBackMethodsCalled)
    }

    @Test
    fun testListenerMethodsOrdering() {
        val oldValue = 1L
        val newValue = 2L
        val ec = EnvironmentConfig()
        ec.memoryUsage = oldValue

        ec.addChangedSettingsListener(object : ConfigSettingChangeListener {
            override fun beforeSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                Assert.assertEquals(oldValue, ec.memoryUsage)
            }

            override fun afterSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                Assert.assertEquals(newValue, ec.memoryUsage)
            }
        })

        ec.memoryUsage = newValue
    }

    @Test
    fun testListenerNotInvokedIfValueHasNotChanged() {
        val value = 1L
        val ec = EnvironmentConfig()
        val callBackMethodsCalled = arrayOfNulls<Boolean>(2)
        ec.memoryUsage = value
        ec.addChangedSettingsListener(object : ConfigSettingChangeListener {
            override fun beforeSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                callBackMethodsCalled[0] = true
            }

            override fun afterSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                callBackMethodsCalled[1] = true
            }
        })

        ec.memoryUsage = value

        Assert.assertArrayEquals(arrayOf<Boolean?>(null, null), callBackMethodsCalled)
    }

    @Test
    fun testListenerInvokedWithContext() {
        val oldValue = 1L
        val newValue = 2L
        val ec = EnvironmentConfig()
        ec.memoryUsage = oldValue

        ec.addChangedSettingsListener(object : ConfigSettingChangeListener {

            private val contextKey = "key"
            private val contextValue = Any()

            override fun beforeSettingChanged(key: String, value: Any, context: MutableMap<String, Any>) {
                Assert.assertTrue(context.isEmpty())
                context.put(contextKey, contextValue)
            }

            override fun afterSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                Assert.assertEquals(contextValue, context[contextKey])
                Assert.assertEquals(1, context.size.toLong())
            }
        })

        ec.memoryUsage = newValue
    }

    @Test
    fun testListenerCanBeSuppressed() {
        val oldValue = 1L
        val newValue = 2L
        val ec = EnvironmentConfig()
        val callBackMethodsCalled = arrayOfNulls<Boolean>(2)
        ec.memoryUsage = oldValue
        ec.addChangedSettingsListener(object : ConfigSettingChangeListener {
            override fun beforeSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                callBackMethodsCalled[0] = true
            }

            override fun afterSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                callBackMethodsCalled[1] = true
            }
        })

        EnvironmentConfig.suppressConfigChangeListenersForThread()
        ec.memoryUsage = newValue
        EnvironmentConfig.resumeConfigChangeListenersForThread()

        Assert.assertArrayEquals(arrayOf<Boolean?>(null, null), callBackMethodsCalled)
    }

    @Test(expected = InvalidSettingException::class)
    fun testUnknownKey() {
        val stringDefaults = HashMap<String, String>()
        stringDefaults.put("unknown.setting.key", null)
        EnvironmentConfig().setSettings(stringDefaults)
    }

    @Test
    fun suppressListenersInListener() {
        val settingFinished = arrayOf<Boolean?>(null)
        val ec = EnvironmentConfig()
        ec.addChangedSettingsListener(object : ConfigSettingChangeListener {
            override fun beforeSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                settingFinished[0] = false
                AbstractConfig.suppressConfigChangeListenersForThread()
            }

            override fun afterSettingChanged(key: String, value: Any, context: Map<String, Any>) {
                settingFinished[0] = true
                AbstractConfig.resumeConfigChangeListenersForThread()
            }
        })
        ec.isGcEnabled = false
        val bool = settingFinished[0]
        Assert.assertNotNull(bool)
        Assert.assertTrue(bool.notNull)
    }
}
