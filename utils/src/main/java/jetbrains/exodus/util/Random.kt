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

/**
 * Non-concurrent replacement of java.util.Random.
 */
class Random {
    private var seed: Long = 0

    constructor() {
        reset()
    }

    constructor(seed: Long) {
        setSeed(seed)
    }

    fun nextInt(): Int {
        return next(32)
    }

    fun nextInt(n: Int): Int {
        require(n > 0) { "n must be positive" }
        if (n and -n == n) // i.e., n is a power of 2
            return (n * next(31).toLong() shr 31).toInt()
        var bits: Int
        var `val`: Int
        do {
            bits = next(31)
            `val` = bits % n
        } while (bits - `val` + (n - 1) < 0)
        return `val`
    }

    fun nextLong(): Long {
        // it's okay that the bottom word remains signed.
        return (next(32).toLong() shl 32) + next(32)
    }

    fun nextBoolean(): Boolean {
        return next(1) != 0
    }

    fun nextFloat(): Float {
        return next(24) / (1 shl 24).toFloat()
    }

    fun nextDouble(): Double {
        return ((next(26).toLong() shl 27) + next(27)) / (1L shl 53).toDouble()
    }

    fun nextBytes(bytes: ByteArray) {
        var i = 0
        val len = bytes.size
        while (i < len) {
            var rnd = nextInt()
            var n = Math.min(len - i, Integer.SIZE / java.lang.Byte.SIZE)
            while (n-- > 0) {
                bytes[i++] = rnd.toByte()
                rnd = rnd shr java.lang.Byte.SIZE
            }
        }
    }

    fun setSeed(seed: Long) {
        this.seed = seed xor multiplier and mask
        for (i in 0..9) {
            nextInt()
        }
    }

    fun reset() {
        setSeed(System.currentTimeMillis())
    }

    fun next(bits: Int): Int {
        seed = seed * multiplier + addend and mask
        return (seed ushr 48 - bits).toInt()
    }

    companion object {
        private const val multiplier = 0x5DEECE66DL
        private const val addend = 0xBL
        private const val mask = (1L shl 48) - 1
    }
}
