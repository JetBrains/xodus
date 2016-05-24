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

import jetbrains.exodus.ConfigSettingChangeListener;
import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Test
    public void testListenerInvoked(){
        final Long oldValue = 1L;
        final Long newValue = 2L;
        final EnvironmentConfig ec = new EnvironmentConfig();
        final Boolean[] callBackMethodsCalled = new Boolean[2];
        ec.setMemoryUsage(oldValue);
        ec.addChangedSettingsListener(new ConfigSettingChangeListener() {
            @Override
            public void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                Assert.assertEquals(EnvironmentConfig.MEMORY_USAGE, key);
                Assert.assertEquals(newValue, value);
                callBackMethodsCalled[0] = true;
            }

            @Override
            public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                Assert.assertEquals(EnvironmentConfig.MEMORY_USAGE, key);
                Assert.assertEquals(newValue, value);
                callBackMethodsCalled[1] = true;
            }
        });

        ec.setMemoryUsage(newValue);

        Assert.assertArrayEquals(new Boolean[]{true, true}, callBackMethodsCalled);
    }

    @Test
    public void testListenerMethodsOrdering(){
        final Long oldValue = 1L;
        final Long newValue = 2L;
        final EnvironmentConfig ec = new EnvironmentConfig();
        ec.setMemoryUsage(oldValue);

        ec.addChangedSettingsListener(new ConfigSettingChangeListener() {
            @Override
            public void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                Assert.assertEquals(oldValue, ec.getMemoryUsage());
            }

            @Override
            public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                Assert.assertEquals(newValue, ec.getMemoryUsage());
            }
        });

        ec.setMemoryUsage(newValue);
    }

    @Test
    public void testListenerNotInvokedIfValueHasNotChanged(){
        final Long value = 1L;
        final EnvironmentConfig ec = new EnvironmentConfig();
        final Boolean[] callBackMethodsCalled = new Boolean[2];
        ec.setMemoryUsage(value);
        ec.addChangedSettingsListener(new ConfigSettingChangeListener() {
            @Override
            public void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                callBackMethodsCalled[0] = true;
            }

            @Override
            public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                callBackMethodsCalled[1] = true;
            }
        });

        ec.setMemoryUsage(value);

        Assert.assertArrayEquals(new Boolean[]{null, null}, callBackMethodsCalled);
    }

    @Test
    public void testListenerInvokedWithContext(){
        final Long oldValue = 1L;
        final Long newValue = 2L;
        final EnvironmentConfig ec = new EnvironmentConfig();
        ec.setMemoryUsage(oldValue);

        ec.addChangedSettingsListener(new ConfigSettingChangeListener() {

            private String contextKey = "key";
            private Object contextValue = new Object();

            @Override
            public void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                Assert.assertTrue(context.isEmpty());
                context.put(contextKey, contextValue);
            }

            @Override
            public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                Assert.assertEquals(contextValue, context.get(contextKey));
                Assert.assertEquals(1, context.size());
            }
        });

        ec.setMemoryUsage(newValue);
    }

    @Test
    public void testListenerCanBeSuppressed(){
        final Long oldValue = 1L;
        final Long newValue = 2L;
        final EnvironmentConfig ec = new EnvironmentConfig();
        final Boolean[] callBackMethodsCalled = new Boolean[2];
        ec.setMemoryUsage(oldValue);
        ec.addChangedSettingsListener(new ConfigSettingChangeListener() {
            @Override
            public void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                callBackMethodsCalled[0] = true;
            }

            @Override
            public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {
                callBackMethodsCalled[1] = true;
            }
        });

        EnvironmentConfig.suppressConfigChangeListenersForThread();
        ec.setMemoryUsage(newValue);
        EnvironmentConfig.resumeConfigChangeListenersForThread();

        Assert.assertArrayEquals(new Boolean[]{null, null}, callBackMethodsCalled);
    }

    @Test(expected = InvalidSettingException.class)
    public void testUnknownKey() {
        final Map<String, String> stringDefaults = new HashMap<>();
        stringDefaults.put("unknown.setting.key", null);
        new EnvironmentConfig().setSettings(stringDefaults);
    }
}
