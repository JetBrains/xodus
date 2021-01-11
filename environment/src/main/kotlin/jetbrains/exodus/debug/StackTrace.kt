/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.debug

import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import java.io.PrintStream

class StackTrace(private val stackTrace: Array<StackTraceElement> =
                         Thread.currentThread().stackTrace.run { copyOfRange(4, size) }) {

    private var hc: Int? = null

    override fun equals(other: Any?) =
            this === other || (other is StackTrace && hc == other.hc && stackTrace.contentEquals(other.stackTrace))

    override fun hashCode() =
            hc ?: stackTrace.fold(0) { hc, e -> hc * 31 + e.hashCode() }.also { hc = it }

    override fun toString() = dumpToString { ps -> toString(ps) }

    fun toString(ps: PrintStream) = stackTrace.forEach { e -> ps.println(e) }
}

/**
 * Map of stack traces to `Long` values. Doesn't implement `java.util.Map`. Is not synchronized.
 */
internal class StackTraceMap {

    private val map = IntHashMap<Entry>()

    operator fun get(stackTrace: StackTrace): Long =
            map[stackTrace.hashCode()].let { entry ->
                if (entry?.stackTrace == stackTrace) entry.value else 0L
            }

    fun add(stackTrace: StackTrace, addend: Long = 1L) {
        val hc = stackTrace.hashCode()
        map[hc].let { entry ->
            if (entry?.stackTrace == stackTrace) {
                entry.value += addend
            } else {
                map[hc] = Entry(stackTrace, addend)
            }
        }
    }

    fun forEach(action: (StackTrace, Long) -> Unit) {
        val entries = mutableListOf<Entry>()
        map.entries.mapTo(entries) { entry -> entry.value }
        entries.apply { sortByDescending { e -> e.value } }.forEach { e ->
            action(e.stackTrace, e.value)
        }
    }

    fun size() = map.size

    fun clear() = map.clear()

    private class Entry(val stackTrace: StackTrace, var value: Long = 0L)
}