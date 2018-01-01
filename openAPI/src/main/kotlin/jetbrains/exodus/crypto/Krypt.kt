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

import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.util.HexUtil
import java.io.InputStream
import java.io.OutputStream

fun newCipherProvider(cipherId: String) = StreamCipherProvider.getProvider(cipherId) ?:
        throw ExodusCryptoException("Failed to load StreamCipherProvider with id = $cipherId")

fun newCipher(cipherId: String): StreamCipher = newCipherProvider(cipherId).newCipher()

fun StreamCipher.with(key: ByteArray, iv: Long) = this.apply { init(key, iv) }

fun toBinaryKey(cipherKey: String): ByteArray {
    if (cipherKey.length and 1 == 1) {
        throw InvalidSettingException("Odd length of hex representation of cipher key")
    }
    return HexUtil.stringToByteArray(cipherKey)
}

infix fun OutputStream.encryptBy(cipher: StreamCipher) = StreamCipherOutputStream(this, cipher)

infix fun InputStream.decryptBy(cipher: StreamCipher) = StreamCipherInputStream(this, cipher)

internal fun StreamCipher.cryptAsInt(b: Byte) = this.crypt(b).toInt() and 0xff
