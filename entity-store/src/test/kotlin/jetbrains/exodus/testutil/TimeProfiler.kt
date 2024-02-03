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
package jetbrains.exodus.testutil

import java.util.concurrent.CopyOnWriteArrayList
import kotlin.collections.maxOrNull
import kotlin.collections.minOrNull
import kotlin.collections.sorted
import kotlin.collections.sum

class TimeProfiler(val name: String) {

    private val times = CopyOnWriteArrayList<Long>()

    fun profile(block: () -> Unit) {
        val start = System.currentTimeMillis()
        block()
        val duration = System.currentTimeMillis() - start
        times.add(duration)
    }

    fun average(): Double {
        val sum = times.sum()
        return sum.toDouble() / times.size
    }

    fun percentile(percentile: Double): Long {
        val sorted =  times.sorted()
        val index = (sorted.size * percentile).toInt()
        return sorted[index]
    }

    fun report() {
        println(
            "Time profiler: $name \n" +
                    "  Total runs: ${times.size} \n" +
                    "  Min: ${times.minOrNull()} ms \n" +
                    "  Max: ${times.maxOrNull()} ms \n" +
                    "  Average: ${average()} ms \n" +
                    "  90th percentile: ${percentile(0.9)} ms \n" +
                    "  95th percentile: ${percentile(0.95)} ms \n" +
                    "  99th percentile: ${percentile(0.99)} ms"
        )
    }

}