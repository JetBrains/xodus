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

import jetbrains.exodus.kotlin.notNull
import java.io.FilterOutputStream
import java.io.OutputStream


class StreamCipherOutputStream(output: OutputStream, private val cipher: StreamCipher) : FilterOutputStream(output) {

    override fun write(b: Int) {
        super.write(cipher.cryptAsInt(b.toByte()))
    }

    override fun write(bytes: ByteArray?) {
        bytes.notNull { "Can't write null array" }.forEach { super.write(cipher.cryptAsInt(it)) }
    }

    override fun write(bytes: ByteArray?, off: Int, len: Int) {
        bytes.notNull { "Can't write null array" }.drop(off).take(len).forEach { super.write(cipher.cryptAsInt(it)) }
    }
}