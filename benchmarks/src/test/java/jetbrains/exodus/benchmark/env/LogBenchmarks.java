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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.log.LogTestsBase;
import jetbrains.exodus.log.LoggableToWrite;
import jetbrains.exodus.util.Random;
import org.junit.Test;

import java.io.IOException;

public class LogBenchmarks extends LogTestsBase {

    private static final Random rnd = new Random();

    static {
        for (int i = 0; i < 10000; ++i) {
            rnd.nextInt();
        }
    }

    @Test
    public void testRandomReadBenchmark() throws IOException {
        randomReadBenchmark(16384, 4096, 20, "LogBenchmark");
    }

    @Test
    public void testRandomReadBenchmark2() throws IOException {
        randomReadBenchmark(16384, 4096, 30, "LogBenchmark2");
    }

    @Test
    public void testRandomReadBenchmark3() throws IOException {
        randomReadBenchmark(16384, 4096, 40, "LogBenchmark3");
    }

    @Test
    public void testRandomReadBenchmark4() throws IOException {
        randomReadBenchmark(16384, 4096, 50, "LogBenchmark4");
    }

    @Test
    public void testRandomReadBenchmark5() throws IOException {
        randomReadBenchmark(16384, 4096, 60, "LogBenchmark5");
    }

    @Test
    public void testRandomReadBenchmark6() throws IOException {
        randomReadBenchmark(16384, 8192, 30, "LogBenchmark6");
    }

    @Test
    public void testRandomReadBenchmark7() throws IOException {
        randomReadBenchmark(16384, 16384, 30, "LogBenchmark7");
    }

    @Test
    public void testRandomReadBenchmark8() throws IOException {
        randomReadBenchmark(16384, 16384, 40, "LogBenchmark8");
    }

    @Test
    public void testRandomReadBenchmark9() throws IOException {
        randomReadBenchmark(16384, 16384, 50, "LogBenchmark9");
    }

    private void randomReadBenchmark(int fileSize, int pageSize, int percent, String valueName) throws IOException {
        initLog(fileSize, pageSize, percent);

        long start;
        final LongArrayList addrs = new LongArrayList();

        start = System.currentTimeMillis();
        final int count = 10000;
        final LoggableToWrite toWrite = LogTestsBase.createOneKbLoggable();
        for (int i = 0; i < count; ++i) {
            addrs.add(getLog().write(toWrite));
        }

        long creationTime = System.currentTimeMillis() - start;
        System.out.println("creation of benchmark log took " + creationTime);
        if (myMessenger != null) {
            myMessenger.putValue(valueName + "_creation", creationTime);
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            getLog().read(addrs.get(rnd.nextInt(addrs.size())));
        }

        long readTime = System.currentTimeMillis() - start;
        System.out.println(percent + "% of memory for cache: random read took " + readTime + ". Cache hit rate: " + ObjectCacheBase.formatHitRate(getLog().getCacheHitRate()));
        if (myMessenger != null) {
            myMessenger.putValue(valueName + "_randomRead", readTime);
        }
        System.out.println();
    }
}
