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

import jetbrains.exodus.core.execution.SharedTimer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;

/**
 * Common class for all statistics items.
 */
public class StatisticsItem implements SharedTimer.ExpirablePeriodicTask {

    @NotNull
    private final String name;
    @NotNull
    private final WeakReference<StatisticsItem> thisRef;
    private long total;
    private double mean; // total per second
    private long lastAdjustTime;
    private long lastAdjustedTotal;

    public StatisticsItem(@NotNull final String name) {
        this.name = name;
        thisRef = new WeakReference<>(this);
        total = 0;
        mean = .0;
        lastAdjustTime = System.currentTimeMillis();
        lastAdjustedTotal = 0;
    }

    @NotNull
    public String getName() {
        return name;
    }

    public long getTotal() {
        synchronized (thisRef) {
            return total;
        }
    }

    public void setTotal(final long total) {
        synchronized (thisRef) {
            this.total = total;
        }
    }

    public double getMean() {
        synchronized (thisRef) {
            return mean;
        }
    }

    @Override
    public boolean isExpired() {
        return thisRef.get() == null;
    }

    @Override
    public void run() {
        final StatisticsItem item = thisRef.get();
        if (item == this) {
            final Long autoUpdatedTotal = getAutoUpdatedTotal();
            if (autoUpdatedTotal != null) {
                setTotal(autoUpdatedTotal);
            }
            adjustMean();
        }
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && name.equals(((StatisticsItem) o).name);

    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public void addTotal(final long addend) {
        synchronized (thisRef) {
            total += addend;
        }
    }

    public void incTotal() {
        addTotal(1L);
    }

    public long getLastAdjustTime() {
        synchronized (thisRef) {
            return lastAdjustTime;
        }
    }

    @Nullable
    protected Long getAutoUpdatedTotal() {
        return null;
    }

    private void adjustMean() {
        final long currentTime = System.currentTimeMillis();
        synchronized (thisRef) {
            mean = (mean + (((double) (total - lastAdjustedTotal)) / (currentTime - lastAdjustTime) / 1000)) / 2;
            lastAdjustTime = currentTime;
            lastAdjustedTotal = total;
        }
    }
}
