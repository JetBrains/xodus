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
package jetbrains.exodus.core.execution

import jetbrains.exodus.TestFor
import org.junit.Assert
import org.junit.Test

class MultiThreadDelegatingJobProcessorTest : JobProcessorTest() {

    override fun createProcessor(): JobProcessor = object : MultiThreadDelegatingJobProcessor("qwa-qwa${Any().hashCode()}", 1, 5000L) {}

    @Test
    @TestFor(issue = "XD-779")
    fun testTimeout() {
        val processor = processor as MultiThreadDelegatingJobProcessor
        SleepJob(processor, 10000L)
        count = 0
        processor.queue(IncrementJob())
        SleepJob(processor, 3000L)
        processor.queue(IncrementJob(2))
        sleep(6500)
        Assert.assertEquals(1, count)
        sleep(2500)
        Assert.assertEquals(3, count)
    }
}