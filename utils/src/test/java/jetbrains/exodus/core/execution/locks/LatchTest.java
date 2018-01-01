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
package jetbrains.exodus.core.execution.locks;

import org.junit.Assert;
import org.junit.Test;

public class LatchTest {


    @Test
    public void test1() throws InterruptedException {
        Latch b = Latch.create();

        b.acquire();
        b.release();
        b.acquire();
        b.release();
    }

    @Test
    public void test3() throws InterruptedException {
        final Latch b = Latch.create();
        final boolean[] testPassed = {false};

        b.acquire();

        new Thread() {
            @Override
            public void run() {
                try {
                    b.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
                testPassed[0] = true;
            }
        }.start();

        Thread.sleep(1000);

        Assert.assertEquals(false, testPassed[0]);

        b.release();

        Thread.sleep(1000);

        Assert.assertEquals(true, testPassed[0]);
    }

    @Test
    public void test4() throws InterruptedException {
        final Latch b = Latch.create();
        final int[] testPassed = {0};

        b.acquire();

        for (int i = 0; i < 33; i++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        b.acquire();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    }
                    testPassed[0] += 1;
                }
            }.start();
        }

        for (int j = 0; j < 33; j++) {
            Assert.assertEquals(j, testPassed[0]);

            b.release();

            Thread.sleep(333);

            Assert.assertEquals(j + 1, testPassed[0]);
        }

    }

}
