/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.management;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.execution.SharedTimer;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Common/base class for metering various statistics.
 */
public class Statistics {

    @NotNull
    private final Map<String, StatisticsItem> items;

    public Statistics() {
        items = new HashMap<>();
    }

    @NotNull
    public StatisticsItem getStatisticsItem(@NotNull final String statisticsName) {
        StatisticsItem result;
        synchronized (items) {
            result = items.get(statisticsName);
        }
        if (result == null) {
            result = createNewItem(statisticsName);
            SharedTimer.registerPeriodicTask(result);
            synchronized (items) {
                // TODO: replace with putIfAbsent() after moving on java 1.8
                if (!items.containsKey(statisticsName)) {
                    items.put(statisticsName, result);
                }
            }
        }
        return result;
    }

    public void registerStatisticsItem(@NotNull final StatisticsItem item) {
        SharedTimer.registerPeriodicTask(item);
        synchronized (items) {
            items.put(item.getName(), item);
        }
    }

    @NotNull
    protected StatisticsItem createNewItem(@NotNull final String statisticsName) {
        return new StatisticsItem(this, statisticsName);
    }
}
