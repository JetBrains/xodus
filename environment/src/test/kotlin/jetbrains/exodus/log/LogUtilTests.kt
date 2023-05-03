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
package jetbrains.exodus.log

import jetbrains.exodus.log.LogUtil.getAddress
import jetbrains.exodus.log.LogUtil.getLogFilename
import org.junit.Assert
import org.junit.Test

class LogUtilTests {
    @Test
    fun testLogFilename() {
        Assert.assertEquals(LogUtil.LOG_FILE_NAME_WITH_EXT_LENGTH.toLong(), getLogFilename(0).length.toLong())
        Assert.assertEquals("00000000000.xd", getLogFilename(0))
        Assert.assertEquals("00000000001.xd", getLogFilename(LogUtil.LOG_BLOCK_ALIGNMENT.toLong()))
        Assert.assertEquals(
            "7vvvvvvvvvv.xd",
            getLogFilename(Long.MAX_VALUE / LogUtil.LOG_BLOCK_ALIGNMENT * LogUtil.LOG_BLOCK_ALIGNMENT)
        )
        Assert.assertEquals("0ok2m96adc9.xd", getLogFilename(LogUtil.LOG_BLOCK_ALIGNMENT * 0x3141592653589L))
    }

    @Test
    fun testLogFileAddress() {
        Assert.assertEquals(0L, getAddress("00000000000.xd"))
        Assert.assertEquals(LogUtil.LOG_BLOCK_ALIGNMENT.toLong(), getAddress("00000000001.xd"))
        Assert.assertEquals(
            Long.MAX_VALUE / LogUtil.LOG_BLOCK_ALIGNMENT * LogUtil.LOG_BLOCK_ALIGNMENT,
            getAddress("7vvvvvvvvvv.xd")
        )
        Assert.assertEquals(LogUtil.LOG_BLOCK_ALIGNMENT * 0x3141592653589L, getAddress("0ok2m96adc9.xd"))
    }

    @Test
    fun testLogFilenameAddressAlignment() {
        var wasException = false
        var logFilename: String? = null
        try {
            logFilename = getLogFilename(1)
        } catch (e: Exception) {
            wasException = true
        }
        Assert.assertNull(logFilename)
        Assert.assertTrue(wasException)
    }

    @Test
    fun testValidLogFileNames() {
        Assert.assertTrue(isValidLogFileName("00000000000.xd"))
        Assert.assertFalse(isValidLogFileName("00000000000"))
        Assert.assertFalse(isValidLogFileName("00000000000.x"))
        Assert.assertFalse(isValidLogFileName("00000000000.db"))
        Assert.assertTrue(isValidLogFileName("00000000001.xd"))
        Assert.assertTrue(isValidLogFileName("7vvvvvvvvvv.xd"))
        Assert.assertTrue(isValidLogFileName("0ok2m96acd9.xd"))
        Assert.assertFalse(isValidLogFileName("wok2m96adc9.xd"))
        Assert.assertFalse(isValidLogFileName("0ok2z96adc9.xd"))
    }

    companion object {
        private fun isValidLogFileName(filename: String): Boolean {
            var result = true
            try {
                getAddress(filename)
            } catch (e: Exception) {
                result = false
            }
            return result
        }
    }
}
