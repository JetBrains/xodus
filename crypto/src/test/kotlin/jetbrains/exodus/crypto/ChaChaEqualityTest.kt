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
package jetbrains.exodus.crypto

import jetbrains.exodus.crypto.streamciphers.CHACHA_CIPHER_ID
import jetbrains.exodus.crypto.streamciphers.JB_CHACHA_CIPHER_ID
import jetbrains.exodus.util.LightOutputStream
import org.junit.Assert
import org.junit.Test
import java.io.ByteArrayInputStream

private val KEY =
        byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
private val IV = 314159262718281828L

class ChaChaEqualityTest {

    @Test
    fun `JB implementation is equal to BC`() {
        val baseOutputStream = LightOutputStream()
        val cipherOutputStream = baseOutputStream encryptBy newCipher(CHACHA_CIPHER_ID).apply { init(KEY, IV) }
        cipherOutputStream.writer().use {
            it.write(RENAT_GILFANOV)
        }
        val cipherInputStream = ByteArrayInputStream(baseOutputStream.bufferBytes, 0, baseOutputStream.size()) decryptBy { newCipher(JB_CHACHA_CIPHER_ID).apply { init(KEY, IV) } }
        Assert.assertEquals(RENAT_GILFANOV, cipherInputStream.reader().readText())
    }
}