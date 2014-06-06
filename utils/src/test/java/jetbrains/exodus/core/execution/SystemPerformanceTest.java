/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.core.execution;

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

public class SystemPerformanceTest {

    @Test
    public void testSystemTimers() {
        final long nanoDelta = 1000L * 1000L * 1000L;
        final long nanoEndTime = System.nanoTime() + nanoDelta;
        int nanoIterations = 0;
        do {
            ++nanoIterations;
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
            System.nanoTime();
        } while (System.nanoTime() < nanoEndTime);
        System.out.println(nanoIterations);
        final long milliDelta = 1000L;
        final long milliEndTime = System.currentTimeMillis() + milliDelta;
        int milliIterations = 0;
        do {
            ++milliIterations;
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
            System.currentTimeMillis();
        } while (System.currentTimeMillis() < milliEndTime);
        System.out.println(milliIterations);
        final double ratio = (double) milliIterations / (double) nanoIterations;
        System.out.println(ratio);
        Assert.assertTrue(ratio < 100);
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void testDateVsCalendar() {
        int dateCreations = 0;
        Date d;
        long end = System.currentTimeMillis() + 1000;
        do {
            d = new Date();
            d = new Date();
            d = new Date();
            d = new Date();
            d = new Date();
            d = new Date();
            d = new Date();
            d = new Date();
            d = new Date();
            d = new Date();
            dateCreations += 10;
        } while (d.getTime() < end);
        System.out.println(dateCreations);
        int calendarCreations = 0;
        end = System.currentTimeMillis() + 1000;
        do {
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            d = Calendar.getInstance().getTime();
            calendarCreations += 10;
        } while (d.getTime() < end);
        System.out.println(calendarCreations);
        final double ratio = (double) dateCreations / (double) calendarCreations;
        System.out.println(ratio);
        Assert.assertTrue(ratio > 1);
    }
}
