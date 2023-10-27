/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.index.util.collections;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class NBHMLLConcurrentInsertTest {
    private static final int THREAD_COUNT = 20;
    private static final int INSERT_COUNT = 100_000;

    @Test
    public void testConcurrentInsert() {
        var nbhmll = new NonBlockingHashMapLongLong();
        var executor = Executors.newCachedThreadPool();

        var futures = new Future<?>[THREAD_COUNT];
        var latch = new CountDownLatch(1);
        for (var i = 0; i < THREAD_COUNT; i++) {
            futures[i] = executor.submit(new NBHMLLConcurrentInsertRunnable(i * INSERT_COUNT,
                    (i + 1) * INSERT_COUNT, latch, nbhmll));
        }
        latch.countDown();

        for (var future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        for (long i = 0; i < THREAD_COUNT * INSERT_COUNT; i++) {
            Assert.assertEquals(2 * i, nbhmll.get(i));
        }
    }

    private static final class NBHMLLConcurrentInsertRunnable implements Callable<Void> {
        private final long startIndex;
        private final long endIndex;

        private final CountDownLatch latch;

        private final NonBlockingHashMapLongLong nbhmll;

        private NBHMLLConcurrentInsertRunnable(long startIndex, long endIndex, CountDownLatch latch,
                                               NonBlockingHashMapLongLong nbhmll) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.latch = latch;
            this.nbhmll = nbhmll;
        }

        @Override
        public Void call() {
            var keys = new long[(int) (endIndex - startIndex)];
            for (long i = startIndex; i < endIndex; i++) {
                keys[(int) (i - startIndex)] = i;
            }
            ArrayUtils.shuffle(keys);

            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            for (var value : keys) {
                nbhmll.put(value, 2 * value);
            }

            return null;

        }
    }
}


