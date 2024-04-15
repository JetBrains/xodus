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