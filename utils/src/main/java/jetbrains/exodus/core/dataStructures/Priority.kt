/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures

@Suppress("HardCodedStringLiteral")
class Priority private constructor(private val value: Int, val description: String?) : Comparable<Priority> {

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o !is Priority) return false
        return value == o.value
    }

    override fun hashCode(): Int {
        return value
    }

    override fun compareTo(o: Priority): Int {
        return value - o.value
    }

    override fun toString(): String {
        return description!!
    }

    companion object {
        val highest = Priority(Int.MAX_VALUE / 2, "The highest possible priority")
        val above_normal = Priority(Int.MAX_VALUE / 4, "The above normal priority")
        val normal = Priority(0, "The normal (default) priority")
        val below_normal = Priority(-above_normal.value, "The below normal priority")
        val lowest = Priority(-highest.value, "The lowest possible priority")
        fun mean(p1: Priority, p2: Priority): Priority {
            // use long in order to avoid integer overflow
            val value = p1.value.toLong() + p2.value.toLong() ushr 1
            return Priority(value.toInt(), null)
        }
    }
}
