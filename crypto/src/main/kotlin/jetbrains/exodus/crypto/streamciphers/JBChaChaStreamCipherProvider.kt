/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.crypto.streamciphers

import jetbrains.exodus.crypto.StreamCipher
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.crypto.toByteArray
import org.bouncycastle.util.Pack
import org.bouncycastle.util.Strings
import kotlin.experimental.xor

const val JB_CHACHA_CIPHER_ID = "jetbrains.exodus.crypto.streamciphers.JBChaChaStreamCipherProvider"

/**
 * ChaCha stream cipher with configurable number of rounds. Is an alternative to the BouncyCastle implementation.
 * Respects [RFC-7539](https://tools.ietf.org/html/rfc7539 RFC-7539).
 *
 * Doesn't require to change IV after being applied to 2^70 bytes of data, since this never is the case in Xodus.
 */
class JBChaChaStreamCipherProvider : StreamCipherProvider() {

    override fun getId() = JB_CHACHA_CIPHER_ID

    override fun newCipher(): StreamCipher = JBChaChaStreamCipher()

    private class JBChaChaStreamCipher(private val rounds: Int = DEFAULT_ROUNDS) : StreamCipher {

        private val state = IntArray(STATE_SIZE)
        private val keyStream = ByteArray(STATE_SIZE * 4) // expanded state, 64 bytes
        private var index = 0

        init {
            if (rounds <= 0 || rounds and 1 != 0) {
                throw IllegalArgumentException("'rounds' must be a positive, even number")
            }
        }

        override fun init(keyBytes: ByteArray, iv: Long) {
            if (keyBytes.size != 32) {
                throw IllegalArgumentException("256 bit key is required")
            }
            val ivBytes = iv.toByteArray(CHACHA_IV_SIZE)
            for (i in 0..3) {
                state[i] = TAU_SIGMA[i + 4]
            }
            // Key
            updateState(keyBytes, 4, 8)
            // IV
            updateState(ivBytes, 13, 3)
            reset()
        }

        override fun crypt(b: Byte): Byte {
            val out = keyStream[index] xor b
            if (++index == STATE_SIZE * 4) {
                index = 0
                if (++state[12] == 0) {
                    throw IllegalStateException("Attempt to increase counter past 2^32.")
                }
                generateKeyStream()
            }
            return out
        }

        fun reset() {
            index = 0
            state[12] = 0
            generateKeyStream()
        }

        private fun generateKeyStream() {
            var x00 = state[0]
            var x01 = state[1]
            var x02 = state[2]
            var x03 = state[3]
            var x04 = state[4]
            var x05 = state[5]
            var x06 = state[6]
            var x07 = state[7]
            var x08 = state[8]
            var x09 = state[9]
            var x10 = state[10]
            var x11 = state[11]
            var x12 = state[12]
            var x13 = state[13]
            var x14 = state[14]
            var x15 = state[15]
            repeat(rounds / 2) {
                x00 += x04
                x12 = rotl16(x12 xor x00)
                x08 += x12
                x04 = rotl12(x04 xor x08)
                x00 += x04
                x12 = rotl8(x12 xor x00)
                x08 += x12
                x04 = rotl7(x04 xor x08)
                x01 += x05
                x13 = rotl16(x13 xor x01)
                x09 += x13
                x05 = rotl12(x05 xor x09)
                x01 += x05
                x13 = rotl8(x13 xor x01)
                x09 += x13
                x05 = rotl7(x05 xor x09)
                x02 += x06
                x14 = rotl16(x14 xor x02)
                x10 += x14
                x06 = rotl12(x06 xor x10)
                x02 += x06
                x14 = rotl8(x14 xor x02)
                x10 += x14
                x06 = rotl7(x06 xor x10)
                x03 += x07
                x15 = rotl16(x15 xor x03)
                x11 += x15
                x07 = rotl12(x07 xor x11)
                x03 += x07
                x15 = rotl8(x15 xor x03)
                x11 += x15
                x07 = rotl7(x07 xor x11)
                x00 += x05
                x15 = rotl16(x15 xor x00)
                x10 += x15
                x05 = rotl12(x05 xor x10)
                x00 += x05
                x15 = rotl8(x15 xor x00)
                x10 += x15
                x05 = rotl7(x05 xor x10)
                x01 += x06
                x12 = rotl16(x12 xor x01)
                x11 += x12
                x06 = rotl12(x06 xor x11)
                x01 += x06
                x12 = rotl8(x12 xor x01)
                x11 += x12
                x06 = rotl7(x06 xor x11)
                x02 += x07
                x13 = rotl16(x13 xor x02)
                x08 += x13
                x07 = rotl12(x07 xor x08)
                x02 += x07
                x13 = rotl8(x13 xor x02)
                x08 += x13
                x07 = rotl7(x07 xor x08)
                x03 += x04
                x14 = rotl16(x14 xor x03)
                x09 += x14
                x04 = rotl12(x04 xor x09)
                x03 += x04
                x14 = rotl8(x14 xor x03)
                x09 += x14
                x04 = rotl7(x04 xor x09)
            }
            intToKeyStream(x00 + state[0], 0)
            intToKeyStream(x01 + state[1], 4)
            intToKeyStream(x02 + state[2], 8)
            intToKeyStream(x03 + state[3], 12)
            intToKeyStream(x04 + state[4], 16)
            intToKeyStream(x05 + state[5], 20)
            intToKeyStream(x06 + state[6], 24)
            intToKeyStream(x07 + state[7], 28)
            intToKeyStream(x08 + state[8], 32)
            intToKeyStream(x09 + state[9], 36)
            intToKeyStream(x10 + state[10], 40)
            intToKeyStream(x11 + state[11], 44)
            intToKeyStream(x12 + state[12], 48)
            intToKeyStream(x13 + state[13], 52)
            intToKeyStream(x14 + state[14], 56)
            intToKeyStream(x15 + state[15], 60)
        }

        private fun updateState(bs: ByteArray, off: Int, count: Int) {
            var byteOffset = 0
            var stateOffset = off
            repeat(count) {
                var result = 0
                for (i in 3 downTo 0) {
                    result = result shl 8
                    result = result or (bs[byteOffset + i].toInt() and 0xff)
                }
                state[stateOffset++] = result
                byteOffset += 4
            }
        }

        private fun intToKeyStream(n: Int, off: Int) {
            var shifted = n
            var keyOffset = off
            repeat(4) {
                keyStream[keyOffset++] = shifted.toByte()
                shifted = shifted shr 8
            }
        }

        private companion object {

            private const val CHACHA_IV_SIZE = 12

            private const val DEFAULT_ROUNDS = 20

            private const val STATE_SIZE = 16

            private val TAU_SIGMA = Pack.littleEndianToInt(Strings.toByteArray("expand 16-byte k" + "expand 32-byte k"), 0, 8)

            private fun rotl16(x: Int) = x shl 16 or x.ushr(16)

            private fun rotl12(x: Int) = x shl 12 or x.ushr(20)

            private fun rotl8(x: Int) = x shl 8 or x.ushr(24)

            private fun rotl7(x: Int) = x shl 7 or x.ushr(25)

        }
    }
}