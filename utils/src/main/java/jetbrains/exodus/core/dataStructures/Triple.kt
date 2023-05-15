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

class Triple<T1, T2, T3>(first: T1, second: T2, third: T3) {
    @JvmField
    val first: T1?
    @JvmField
    val second: T2?
    @JvmField
    val third: T3?

    init {
        this.first = first
        this.second = second
        this.third = third
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val triple = o as Triple<*, *, *>
        if (first != triple.first) return false
        return if (second != triple.second) false else third == triple.third
    }

    override fun hashCode(): Int {
        var result = first?.hashCode() ?: 0
        result = 31 * result + (second?.hashCode() ?: 0)
        result = 31 * result + (third?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "Triple{" +
                "first=" + first +
                ", second=" + second +
                ", third=" + third +
                '}'
    }
}
