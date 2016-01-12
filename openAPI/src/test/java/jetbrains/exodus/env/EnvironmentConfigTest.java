/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.env;

import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class EnvironmentConfigTest {

    @Test
    public void testCloneDefaults() {
        final Map<String, Object> defaults = EnvironmentConfig.DEFAULT.getSettings();
        final EnvironmentConfig ec = new EnvironmentConfig();
        final Map<String, String> stringDefaults = new HashMap<>();
        for (final Map.Entry<String, Object> entry : defaults.entrySet()) {
            stringDefaults.put(entry.getKey(), entry.getValue().toString());
        }
        ec.setSettings(stringDefaults);
        for (final Map.Entry<String, Object> entry : defaults.entrySet()) {
            Assert.assertEquals(entry.getValue(), ec.getSetting(entry.getKey()));
        }
    }

    @Test(expected = InvalidSettingException.class)
    public void testUnknownKey() {
        final Map<String, String> stringDefaults = new HashMap<>();
        stringDefaults.put("unknown.setting.key", null);
        new EnvironmentConfig().setSettings(stringDefaults);
    }
}
