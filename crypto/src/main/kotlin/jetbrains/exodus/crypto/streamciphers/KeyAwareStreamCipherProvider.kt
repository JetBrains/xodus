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
package jetbrains.exodus.crypto.streamciphers

import jetbrains.exodus.crypto.StreamCipher
import jetbrains.exodus.crypto.StreamCipherProvider
import org.bouncycastle.crypto.params.KeyParameter

/**
 * This [StreamCipherProvider] caches key information in order to reduce GC traffic on cipher initialization.
 */
abstract class KeyAwareStreamCipherProvider : StreamCipherProvider() {

    private var keyInfo = ByteArray(0) to KeyParameter(ByteArray(0))

    internal fun getKeyParameter(key: ByteArray): KeyParameter {
        var keyInfo = keyInfo
        if (keyInfo.first !== key) {
            keyInfo = key to KeyParameter(key)
            this.keyInfo = keyInfo
        }
        return keyInfo.second
    }

    internal abstract class KeyAwareStreamCipher(private val provider: KeyAwareStreamCipherProvider) : StreamCipher {

        internal fun getKeyParameter(key: ByteArray) = provider.getKeyParameter(key)
    }
}