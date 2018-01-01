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
    private final WeakReference<Statistics> statisticsRef;
    private long total;
    private double mean; // total per second
    private long lastAdjustTime;
    private long lastAdjustedTotal;

    public StatisticsItem(@NotNull final Statistics statistics) {
        statisticsRef = new WeakReference<>(statistics);
        total = 0;
        mean = .0;
        lastAdjustTime = System.currentTimeMillis();
        lastAdjustedTotal = 0;
    }

    public long getTotal() {
        synchronized (statisticsRef) {
            return total;
        }
    }

    public void setTotal(final long total) {
        synchronized (statisticsRef) {
            this.total = total;
        }
    }

    public double getMean() {
        synchronized (statisticsRef) {
            return mean;
        }
    }

    @Override
    public boolean isExpired() {
        return statisticsRef.get() == null;
    }

    @Override
    public void run() {
        final Long autoUpdatedTotal = getAutoUpdatedTotal();
        if (autoUpdatedTotal != null) {
            setTotal(autoUpdatedTotal);
        }
        adjustMean();
    }

    public void addTotal(final long addend) {
        synchronized (statisticsRef) {
            total += addend;
        }
    }

    public void incTotal() {
        addTotal(1L);
    }

    @Nullable
    protected Statistics getStatistics() {
        return statisticsRef.get();
    }

    @Nullable
    protected Long getAutoUpdatedTotal() {
        return null;
    }

    private void adjustMean() {
        if (total > lastAdjustedTotal) {
            final long currentTime = System.currentTimeMillis();
            synchronized (statisticsRef) {
                if (total > lastAdjustedTotal) {
                    mean = (mean + (((double) (((total - lastAdjustedTotal)) * 1000L)) / ((double) (currentTime - lastAdjustTime)))) / 2;
                    lastAdjustTime = currentTime;
                    lastAdjustedTotal = total;
                }
            }
        }
    }
}
