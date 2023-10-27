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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class NBHMLLConcurrentInsertReplaceIfTest {
    private static final int THREAD_INSERTION_COUNT = 10;
    private static final int THREAD_REPLACE_COUNT = 10;
    private static final int INSERT_COUNT = 100_000;

    @Test
    public void testConcurrentInsertReplaceIf() {
        var nbhmll = new NonBlockingHashMapLongLong();
        try (var executor = Executors.newCachedThreadPool()) {
            var insertFutures = new Future<?>[THREAD_INSERTION_COUNT];
            var removeFutures = new Future<?>[THREAD_REPLACE_COUNT];

            var latch = new CountDownLatch(1);
            var added = new ConcurrentHashMap<Long, Long>();

            for (var i = 0; i < THREAD_INSERTION_COUNT; i++) {
                insertFutures[i] = executor.submit(new NBHMLLConcurrentInsertRunnable(i * INSERT_COUNT,
                        (i + 1) * INSERT_COUNT, latch, nbhmll, added));
            }

            var lastIteration = new AtomicBoolean(false);
            for (var i = 0; i < THREAD_REPLACE_COUNT; i++) {
                removeFutures[i] = executor.submit(new NBHMLLConcurrentReplaceRunnable(latch, nbhmll, added,
                        lastIteration));
            }

            latch.countDown();

            for (var future : insertFutures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            lastIteration.set(true);

            for (var future : removeFutures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            for (long i = 0; i < THREAD_INSERTION_COUNT * INSERT_COUNT; i++) {
                Assert.assertEquals("key  = " + i, i % 2 == 0 ? i + 1 : 2 * i, nbhmll.get(i));
            }
        }
    }

    private static final class NBHMLLConcurrentInsertRunnable implements Callable<Void> {
        private final long startIndex;
        private final long endIndex;

        private final CountDownLatch latch;

        private final NonBlockingHashMapLongLong nbhmll;

        private final ConcurrentHashMap<Long, Long> added;

        private NBHMLLConcurrentInsertRunnable(long startIndex, long endIndex, CountDownLatch latch,
                                               NonBlockingHashMapLongLong nbhmll,
                                               ConcurrentHashMap<Long, Long> added) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.latch = latch;
            this.nbhmll = nbhmll;
            this.added = added;
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

            for (var key : keys) {
                nbhmll.put(key, 2 * key);
                added.put(key, 2 * key);
            }

            return null;

        }
    }

    private static final class NBHMLLConcurrentReplaceRunnable implements Callable<Void> {
        private final CountDownLatch latch;

        private final NonBlockingHashMapLongLong nbhmll;

        private final ConcurrentHashMap<Long, Long> added;

        private final AtomicBoolean lastIteration;

        private NBHMLLConcurrentReplaceRunnable(CountDownLatch latch,
                                                NonBlockingHashMapLongLong nbhmll,
                                                ConcurrentHashMap<Long, Long> added, AtomicBoolean lastIteration) {
            this.latch = latch;
            this.nbhmll = nbhmll;
            this.added = added;
            this.lastIteration = lastIteration;
        }

        @Override
        public Void call() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            do {
                for (var key : added.keySet()) {
                    var value = nbhmll.get(key);
                    if (key % 2 == 0 && value == 2 * key) {
                        nbhmll.replace(key, value, key + 1);
                    }

                    added.remove(key);
                }
            } while (!lastIteration.get());

            for (var key : added.keySet()) {
                var value = nbhmll.get(key);
                if (key % 2 == 0 && value == 2 * key) {
                    nbhmll.replace(key, value, key + 1);
                }
                added.remove(key);
            }

            return null;
        }
    }
}
