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
package jetbrains.exodus.benchmark.dataStructures;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.ConcurrentStablePriorityQueue;
import jetbrains.exodus.core.dataStructures.PriorityQueue;
import jetbrains.exodus.core.dataStructures.StablePriorityQueue;
import org.junit.Test;

public class PriorityQueueBenchmark {

    private static final int COUNT = 1000000;

    @Test
    public void benchmarkOrdered() {
        TestUtil.time("StablePriorityQueue ordered test", new Runnable() {
            @Override
            public void run() {
                final PriorityQueue<Integer, Integer> queue = new StablePriorityQueue<>();
                for (int i = 0; i < COUNT; ++i) {
                    queue.lock();
                    queue.push(i, i);
                    queue.unlock();
                }
            }
        });
        TestUtil.time("ConcurrentStablePriorityQueue ordered test", new Runnable() {
            @Override
            public void run() {
                final PriorityQueue<Integer, Integer> queue = new ConcurrentStablePriorityQueue<>();
                for (int i = 0; i < COUNT; ++i) {
                    queue.push(i, i);
                }
            }
        });
    }

    @Test
    public void benchmarkReverseOrdered() {
        TestUtil.time("StablePriorityQueue reverse ordered test", new Runnable() {
            @Override
            public void run() {
                final PriorityQueue<Integer, Integer> queue = new StablePriorityQueue<>();
                for (int i = COUNT; i > 0; --i) {
                    queue.lock();
                    queue.push(i, i);
                    queue.unlock();
                }
            }
        });
        TestUtil.time("ConcurrentStablePriorityQueue reverse ordered test", new Runnable() {
            @Override
            public void run() {
                final PriorityQueue<Integer, Integer> queue = new ConcurrentStablePriorityQueue<>();
                for (int i = COUNT; i > 0; --i) {
                    queue.push(i, i);
                }
            }
        });
    }
}
