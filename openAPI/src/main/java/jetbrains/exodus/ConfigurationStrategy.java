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
package jetbrains.exodus;

/**
 * Defines a way how {@link AbstractConfig} should be initially set. E.g., {@link #IGNORE} creates
 * default config, {@link #SYSTEM_PROPERTY} creates config filled up with values of system properties.
 */
public interface ConfigurationStrategy {

    ConfigurationStrategy IGNORE = new ConfigurationStrategy() {
        @Override
        public String getProperty(String key) {
            return null;
        }
    };

    ConfigurationStrategy SYSTEM_PROPERTY = new ConfigurationStrategy() {
        @Override
        public String getProperty(String key) {
            return System.getProperty(key);
        }
    };

    String getProperty(String key);
}
