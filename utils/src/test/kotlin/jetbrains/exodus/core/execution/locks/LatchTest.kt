/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.core.execution.locks

import org.junit.Assert
import org.junit.Test

class LatchTest {

    @Test
    fun test1() {
        val b = Latch.create()

        b.acquire()
        b.release()
        b.acquire()
        b.release()
    }

    @Test
    fun test3() {
        val b = Latch.create()
        val testPassed = booleanArrayOf(false)

        b.acquire()

        object : Thread() {
            override fun run() {
                try {
                    b.acquire()
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    e.printStackTrace()
                }
                testPassed[0] = true
            }
        }.start()
        Thread.sleep(1000)
        Assert.assertEquals(false, testPassed[0])
        b.release()
        Thread.sleep(1000)
        Assert.assertEquals(true, testPassed[0])
    }

    @Test
    fun test4() {
        val b = Latch.create()
        val testPassed = intArrayOf(0)

        b.acquire()

        for (i in 0..32) {
            object : Thread() {
                override fun run() {
                    try {
                        b.acquire()
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        e.printStackTrace()
                    }
                    testPassed[0] += 1
                }
            }.start()
        }

        for (j in 0..32) {
            Assert.assertEquals(j.toLong(), testPassed[0].toLong())
            b.release()
            Thread.sleep(333)
            Assert.assertEquals((j + 1).toLong(), testPassed[0].toLong())
        }
    }
}
