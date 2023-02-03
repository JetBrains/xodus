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
package jetbrains.exodus.crypto

import jetbrains.exodus.kotlin.notNull
import java.io.BufferedInputStream
import java.io.FilterInputStream
import java.io.InputStream

val InputStream.asBuffered: InputStream get() = this as? BufferedInputStream ?: BufferedInputStream(this)

class StreamCipherInputStream(input: InputStream, private val cipherGetter: () -> StreamCipher) : FilterInputStream(input) {

    private var cipher: StreamCipher = cipherGetter()
    private var position = 0
    private var savedPosition = 0

    override fun read(): Int {
        val b = super.read()
        if (b == -1) {
            return -1
        }
        return cipher.cryptAsInt(b.toByte()).apply { ++position }
    }

    override fun read(bytes: ByteArray?): Int {
        val b = bytes.notNull { "Can't read into null array" }
        return read(b, 0, b.size)
    }

    override fun read(bytes: ByteArray?, off: Int, len: Int): Int {
        val b = bytes.notNull { "Can't read into null array" }
        val read = super.read(b, off, len)
        if (read > 0) {
            for (i in off until read + off) {
                b[i] = cipher.crypt(b[i])
            }
            position += read
        }
        return read
    }

    override fun reset() {
        super.reset()
        cipher = cipherGetter()
        // skip savedPosition bytes
        repeat(savedPosition) {
            cipher.crypt(0)
        }
        position = savedPosition
    }

    override fun mark(readlimit: Int) {
        super.mark(readlimit)
        savedPosition = position
    }

    val source: InputStream
        get() {
            var result = `in`
            while (result is FilterInputStream) {
                result = `in`
            }
            return result
        }
}