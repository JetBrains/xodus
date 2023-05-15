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
package jetbrains.exodus.util

object ByteUtil {
    fun or(b1: Byte, b2: Byte): Byte {
        return (b1.toInt() or b2.toInt()).toByte()
    }

    fun and(b1: Byte, b2: Byte): Byte {
        return (b1.toInt() and b2.toInt()).toByte()
    }

    fun or(b1: Short, b2: Short): Short {
        return (b1.toInt() or b2.toInt()).toShort()
    }

    fun and(b1: Short, b2: Short): Short {
        return (b1.toInt() and b2.toInt()).toShort()
    }

    fun xor(b1: Short, b2: Short): Short {
        return (b1.toInt() xor b2.toInt()).toShort()
    }

    fun massOr(vararg s: Short): Short {
        var mask: Short = 0
        for (i0 in s) {
            mask = (mask.toInt() or i0.toInt()).toShort()
        }
        return mask
    }

    fun shiftLeft(operand: Int, step: Int): Int {
        return operand shl step
    }

    fun shiftLeft(operand: Short, step: Short): Short {
        return (operand.toInt() shl step.toInt()).toShort()
    }

    fun xor(i1: Int, i2: Int): Int {
        return i1 xor i2
    }

    fun and(i1: Int, i2: Int): Int {
        return i1 and i2
    }

    fun or(i1: Int, i2: Int): Int {
        return i1 or i2
    }

    fun not(i: Int): Int {
        return i.inv()
    }

    fun massOr(vararg i: Int): Int {
        var mask = 0
        for (i0 in i) {
            mask = mask or i0
        }
        return mask
    }
}
