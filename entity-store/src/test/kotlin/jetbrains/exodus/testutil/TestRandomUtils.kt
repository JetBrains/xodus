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

import kotlin.collections.elementAt
import kotlin.ranges.coerceAtLeast
import kotlin.ranges.coerceAtMost

fun <E> Collection<E>.randomGaussian(rnd: java.util.Random): E {
    // The random.nextGaussian() generates random numbers by following the standard Gaussian distribution,
    // also known as the normal distribution, where the mean is 0 and standard deviation is 1.
    // This method generates numbers so that about 68% of the time, they fall within -1 and 1,
    // about 95% of the time they fall within -2 and 2, and about 99.7% of the time they fall within -3 and 3.
    // By dividing the value by 3, we are effectively reducing the standard deviation so that 99.7% of the time,
    // the value will be within -1 and 1. Which means we bring +- 99.7% variation
    // (3-sigma variation in standard normal distribution) to +- 1.
    val gaussian = rnd.nextGaussian() / 3
    val index = (gaussian * size / 2 + size / 2).toInt()
        .coerceAtLeast(0)
        .coerceAtMost(size - 1)
    return elementAt(index)
}