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

class Pair<F, S>(first: F, second: S) {
    @JvmField
    val first: F?
    @JvmField
    val second: S?

    init {
        this.first = first
        this.second = second
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val pair = o as Pair<*, *>
        return if (first != pair.first) false else second == pair.second
    }

    override fun hashCode(): Int {
        var result = first?.hashCode() ?: 0
        result = 31 * result + (second?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}'
    }
}
