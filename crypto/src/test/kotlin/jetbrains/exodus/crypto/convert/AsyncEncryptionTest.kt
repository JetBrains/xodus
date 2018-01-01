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
package jetbrains.exodus.crypto.convert

import jetbrains.exodus.crypto.RENAT_GILFANOV
import jetbrains.exodus.crypto.cryptBlocksImmutable
import jetbrains.exodus.crypto.newCipherProvider
import jetbrains.exodus.crypto.streamciphers.SALSA20_CIPHER_ID
import jetbrains.exodus.crypto.toBinaryKey
import jetbrains.exodus.util.HexUtil
import org.junit.Assert.assertEquals
import org.junit.Test

class AsyncEncryptionTest {
    val salsa get() = newCipherProvider(SALSA20_CIPHER_ID)
    val key get() = toBinaryKey("0102030405060708090A0B0C0D0E0F10")
    val basicIV = 314159262718281828L
    val address = 1214L
    val alignment = 512

    @Test
    fun testCipherAsync() {
        val data = RENAT_GILFANOV.toByteArray()

        val encrypted = encryptStringByteByByte(data, false)

        println(String(encrypted))

        val decrypted = encryptStringByteByByte(encrypted, false)

        String(decrypted).let {
            println(it)
            assertEquals(RENAT_GILFANOV, it)
        }
    }

    @Test
    fun testCipherAsyncChunked() {
        val data = RENAT_GILFANOV.toByteArray()

        val encryptedInPlace = cryptBlocksImmutable(salsa, key, basicIV, address, data, 0, data.size, alignment)

        val encrypted = encryptStringByteByByte(data, true)

        println(String(encrypted))

        assertEquals(HexUtil.byteArrayToString(encryptedInPlace), HexUtil.byteArrayToString(encrypted))

        val decrypted = encryptStringByteByByte(encrypted, true)

        String(decrypted).let {
            println(it)
            assertEquals(RENAT_GILFANOV, it)
        }
    }

    private fun encryptStringByteByByte(data: ByteArray, chunked: Boolean): ByteArray {
        val encrypted = ByteArray(data.size)

        ScytaleEngine(makeListener(encrypted), salsa, key, basicIV, alignment, 1024).use { engine ->
            engine.start()

            val header = makeDummyHeader(data.size, chunked)
            engine.put(header)

            data.forEach {
                val chunk = engine.alloc()
                chunk[0] = it
                engine.put(FileChunk(header, 1, chunk))
            }

            engine.put(EndChunk)
        }
        return encrypted
    }

    private fun makeListener(output: ByteArray) = object : EncryptListener {
        var index = 0

        override fun onFile(header: FileHeader) = Unit

        override fun onFileEnd(header: FileHeader) = Unit

        override fun onData(header: FileHeader, size: Int, data: ByteArray) {
            val start = index
            val end = start + size
            if (end > output.size) {
                throw IllegalStateException()
            }
            System.arraycopy(data, 0, output, start, size)
            index = end
        }
    }

    private fun makeDummyHeader(size: Int, chunked: Boolean)
            = FileHeader("", "", size.toLong(), 0, address, chunked, true)
}
