package jetbrains.exodus;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public interface ConfigSettingChangeListener {

    void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context);

    void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context);

    class Adapter implements  ConfigSettingChangeListener {

        @Override
        public void beforeSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {

        }

        @Override
        public void afterSettingChanged(@NotNull String key, @NotNull Object value, @NotNull Map<String, Object> context) {

        }
    }
}
