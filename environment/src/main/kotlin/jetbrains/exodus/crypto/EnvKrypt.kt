/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.crypto

/**
 * Crypts byte array in-place by blocks of length `alignment`.
 * Can be applied only to byte arrays which cannot be re-used for reading.
 */
fun cryptBlocksMutable(cipherProvider: StreamCipherProvider,
                       cipherKey: ByteArray,
                       basicIV: Long,
                       address: Long,
                       bytes: ByteArray,
                       offset: Int,
                       length: Int,
                       alignment: Int) {
    cryptBlocksImpl(cipherProvider, cipherKey, basicIV, address, bytes, offset, length, bytes, offset, alignment)
}

/**
 * Crypts immutable byte array by blocks of length `alignment`.
 */
fun cryptBlocksImmutable(cipherProvider: StreamCipherProvider,
                         cipherKey: ByteArray,
                         basicIV: Long,
                         address: Long,
                         bytes: ByteArray,
                         offset: Int,
                         length: Int,
                         alignment: Int): ByteArray {
    return ByteArray(length).also {
        cryptBlocksImpl(cipherProvider, cipherKey, basicIV, address, bytes, offset, length, it, 0, alignment)
    }
}

/**
 * Crypts byte array by blocks of length `alignment`.
 */
private fun cryptBlocksImpl(cipherProvider: StreamCipherProvider,
                            cipherKey: ByteArray,
                            basicIV: Long,
                            address: Long,
                            input: ByteArray,
                            inputOffset: Int,
                            length: Int,
                            output: ByteArray,
                            outputOffset: Int,
                            alignment: Int) {
    var iv = basicIV + ((address + inputOffset) / alignment)
    var inputOff = inputOffset
    var len = length
    var outputOff = outputOffset

    while (len > 0) {
        val offsetInBlock = inputOff % alignment
        val blockLen = minOf(alignment - offsetInBlock, len)
        val cipher = cipherProvider.newCipher().apply {
            init(cipherKey, iv.asHashedIV())
        }
        // if offset is not the left bound of a block then the cipher should skip some bytes
        if (offsetInBlock > 0) {
            repeat(offsetInBlock, {
                cipher.crypt(0)
            })
        }
        repeat(blockLen, {
            output[outputOff++] = cipher.crypt(input[inputOff++])
        })
        ++iv
        len -= blockLen
    }
}

// Donald Knuth's 64-bit linear congruent generator (https://en.wikipedia.org/wiki/Linear_congruential_generator)
fun Long.asHashedIV() = this * 6364136223846793005 + 1442695040888963407