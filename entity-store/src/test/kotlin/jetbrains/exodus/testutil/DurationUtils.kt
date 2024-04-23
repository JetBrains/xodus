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

import java.time.Duration

fun Duration.toHumanString(): String {
    val days = toDaysPart().toInt().toTimeUnit("d")
    val hours = toHoursPart().toTimeUnit("h")
    val minutes = toMinutesPart().toTimeUnit("m")
    val seconds = toSecondsPart().toTimeUnit("s")
    val millis = toMillisPart().toTimeUnit("ms")
    val units = listOfNotNull(days, hours, minutes, seconds, millis)
    return if (units.isEmpty()) {
        "0ms"
    } else {
        units.joinToString(" ")
    }
}

private fun Int.toTimeUnit(unit: String): String? {
    return if (this > 0) {
        "$this$unit"
    } else {
        null
    }
}