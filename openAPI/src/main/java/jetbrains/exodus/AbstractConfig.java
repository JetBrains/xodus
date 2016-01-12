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
package jetbrains.exodus;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public abstract class AbstractConfig {

    @NonNls
    private static final String UNSUPPORTED_TYPE_ERROR_MSG = "Unsupported value type";

    @NotNull
    private final Map<String, Object> settings;
    @NotNull
    private final Set<ChangedSettingsListener> listeners;

    protected AbstractConfig(@NotNull final Pair<String, Object>[] props, @NotNull final ConfigurationStrategy strategy) {
        settings = new HashMap<>();
        listeners = new LinkedHashSet<>();
        for (final Pair<String, Object> prop : props) {
            final String propName = prop.getFirst();
            final Object defaultValue = prop.getSecond();
            final Object value;
            final Class<?> clazz = defaultValue.getClass();
            if (clazz == Boolean.class) {
                value = getBoolean(strategy, propName, (Boolean) defaultValue);
            } else if (clazz == Integer.class) {
                value = getInteger(strategy, propName, (Integer) defaultValue);
            } else if (clazz == Long.class) {
                value = getLong(strategy, propName, (Long) defaultValue);
            } else {
                throw new ExodusException(UNSUPPORTED_TYPE_ERROR_MSG);
            }
            setSetting(propName, value);
        }
    }

    public Object getSetting(@NotNull final String key) {
        return settings.get(key);
    }

    public AbstractConfig setSetting(@NotNull final String key, @NotNull final Object value) {
        settings.put(key, value);
        fireChangedSettingsListeners(key);
        return this;
    }

    public Map<String, Object> getSettings() {
        return Collections.unmodifiableMap(settings);
    }

    public void addChangedSettingsListener(@NotNull final ChangedSettingsListener listener) {
        listeners.add(listener);
    }

    public void removeChangedSettingsListener(@NotNull final ChangedSettingsListener listener) {
        listeners.remove(listener);
    }

    public void setSettings(@NotNull final Map<String, String> settings) throws InvalidSettingException {
        final StringBuilder errorMessage = new StringBuilder();
        for (final Map.Entry<String, String> entry : settings.entrySet()) {
            final String key = entry.getKey();
            final Object oldValue = getSetting(key);
            if (oldValue == null) {
                appendLineFeed(errorMessage);
                errorMessage.append("Unknown setting key: ");
                errorMessage.append(key);
                continue;
            }
            final String value = entry.getValue();
            final Object newValue;
            final Class<?> clazz = oldValue.getClass();
            try {
                if (clazz == Boolean.class) {
                    newValue = Boolean.valueOf(value);
                } else if (clazz == Integer.class) {
                    newValue = Integer.decode(value);
                } else if (clazz == Long.class) {
                    newValue = Long.decode(value);
                } else {
                    appendLineFeed(errorMessage);
                    errorMessage.append(UNSUPPORTED_TYPE_ERROR_MSG);
                    errorMessage.append(": ");
                    errorMessage.append(clazz);
                    continue;
                }
                setSetting(key, newValue);
            } catch (NumberFormatException ignore) {
                appendLineFeed(errorMessage);
                errorMessage.append(UNSUPPORTED_TYPE_ERROR_MSG);
                errorMessage.append(": ");
                errorMessage.append(clazz);
            }
        }
        if (errorMessage.length() > 0) {
            throw new InvalidSettingException(errorMessage.toString());
        }
    }

    private void fireChangedSettingsListeners(@NotNull final String settingName) {
        for (final ChangedSettingsListener listener : listeners) {
            listener.settingChanged(settingName);
        }
    }

    private static boolean getBoolean(@NotNull final ConfigurationStrategy strategy,
                                      @NotNull final String propName, final boolean defaultValue) {
        final String value = strategy.getProperty(propName);
        return value == null ? defaultValue : "true".equalsIgnoreCase(value);
    }

    private static Integer getInteger(@NotNull final ConfigurationStrategy strategy,
                                      @NotNull String propName, Integer defaultValue) {
        final String v = strategy.getProperty(propName);
        if (v != null) {
            try {
                return Integer.decode(v);
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    private static Long getLong(@NotNull final ConfigurationStrategy strategy,
                                @NotNull String propName, Long defaultValue) {
        final String v = strategy.getProperty(propName);
        if (v != null) {
            try {
                return Long.decode(v);
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    private static void appendLineFeed(@NotNull final StringBuilder builder) {
        if (builder.length() > 0) {
            builder.append('\n');
        }
    }

    public interface ChangedSettingsListener {

        void settingChanged(@NotNull final String settingName);
    }
}
