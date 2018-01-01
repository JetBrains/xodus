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
package jetbrains.exodus.crypto;

import org.jetbrains.annotations.NotNull;

/**
 * Database ciphering facility uses stream ciphers. Any implementation of stream cipher which can be used by
 * database engine should implement this interface. To create stream ciphers, implement {@linkplain StreamCipherProvider}
 * as a service provider interface.
 *
 * @see StreamCipherProvider
 */
public interface StreamCipher {

    /**
     * Initializes the cipher from scratch with specified key and initialization vector. Call to
     * {@linkplain #crypt(byte)} will fail if the cipher is not initialized.
     *
     * @param key cipher key of a length expected by particular implementation
     * @param iv  64-bit initialization vector
     */
    void init(@NotNull final byte[] key, final long iv);

    /**
     * Encrypts/decrypts specified byte.
     *
     * @param b byte to encrypt/decrypt
     * @return encrypted/decrypted byte
     */
    byte crypt(final byte b);
}