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
package jetbrains.exodus.log;

import org.junit.Assert;
import org.junit.Test;

public class LogUtilTests {

    @Test
    public void testLogFilename() {
        Assert.assertEquals(LogUtil.LOG_FILE_NAME_WITH_EXT_LENGTH, LogUtil.getLogFilename(0).length());
        Assert.assertEquals("00000000000.xd", LogUtil.getLogFilename(0));
        Assert.assertEquals("00000000001.xd", LogUtil.getLogFilename(LogUtil.LOG_BLOCK_ALIGNMENT));
        Assert.assertEquals("7vvvvvvvvvv.xd", LogUtil.getLogFilename(Long.MAX_VALUE / LogUtil.LOG_BLOCK_ALIGNMENT * LogUtil.LOG_BLOCK_ALIGNMENT));
        Assert.assertEquals("0ok2m96adc9.xd", LogUtil.getLogFilename(LogUtil.LOG_BLOCK_ALIGNMENT * 0x3141592653589L));
    }

    @Test
    public void testLogFileAddress() {
        Assert.assertEquals(0L, LogUtil.getAddress("00000000000.xd"));
        Assert.assertEquals((long) LogUtil.LOG_BLOCK_ALIGNMENT, LogUtil.getAddress("00000000001.xd"));
        Assert.assertEquals(Long.MAX_VALUE / LogUtil.LOG_BLOCK_ALIGNMENT * LogUtil.LOG_BLOCK_ALIGNMENT, LogUtil.getAddress("7vvvvvvvvvv.xd"));
        Assert.assertEquals(LogUtil.LOG_BLOCK_ALIGNMENT * 0x3141592653589L, LogUtil.getAddress("0ok2m96adc9.xd"));
    }

    @Test
    public void testLogFilenameAddressAlignment() {
        boolean wasException = false;
        String logFilename = null;
        try {
            logFilename = LogUtil.getLogFilename(1);
        } catch (Exception e) {
            wasException = true;
        }
        Assert.assertNull(logFilename);
        Assert.assertTrue(wasException);
    }

    @Test
    public void testValidLogFileNames() {
        Assert.assertTrue(isValidLogFileName("00000000000.xd"));
        Assert.assertFalse(isValidLogFileName("00000000000"));
        Assert.assertFalse(isValidLogFileName("00000000000.x"));
        Assert.assertFalse(isValidLogFileName("00000000000.db"));
        Assert.assertTrue(isValidLogFileName("00000000001.xd"));
        Assert.assertTrue(isValidLogFileName("7vvvvvvvvvv.xd"));
        Assert.assertTrue(isValidLogFileName("0ok2m96acd9.xd"));
        Assert.assertFalse(isValidLogFileName("wok2m96adc9.xd"));
        Assert.assertFalse(isValidLogFileName("0ok2z96adc9.xd"));
    }

    private static boolean isValidLogFileName(final String filename) {
        boolean result = true;
        try {
            LogUtil.getAddress(filename);
        } catch (Exception e) {
            result = false;
        }
        return result;
    }
}
