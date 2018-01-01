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

import jetbrains.exodus.env.EnvironmentConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ServiceLoader;

/**
 * Service provider interface for creation instances of {@linkplain StreamCipher}. Id of the cipher defines stream
 * cipher type (algorithm). Id can be an arbitrary string, but it's recommended to use the fully qualified name of the
 * StreamCipherProvider implementation for easier and more transparent configuration using {@linkplain EnvironmentConfig}.
 */
public abstract class StreamCipherProvider {

    /**
     * Id of the cipher defines stream cipher type (algorithm). Id is an arbitrary string, but it's recommended to use
     * a fully qualified name of the StreamCipherProvider implementation for easier and more transparent configuration
     * using {@linkplain EnvironmentConfig}.
     *
     * @return stream cipher id
     */
    @NotNull
    public abstract String getId();

    /**
     * Creates new instance of stream cipher. Use {@linkplain StreamCipher#init(byte[], long)} to initialize
     * encryption/decryption. Use {@linkplain StreamCipher#crypt(byte)} to encrypt/decrypt bytes.
     *
     * @return new instance of stream cipher
     */
    @NotNull
    public abstract StreamCipher newCipher();

    /**
     * Gets a {@code StreamCipherProvider} implementation by specified id.
     *
     * @param id id of {@code StreamCipherProvider}
     * @return {@code StreamCipherProvider} implementation of {@code null} if the service could not be loaded
     */
    @Nullable
    public static StreamCipherProvider getProvider(@NotNull final String id) {
        for (final StreamCipherProvider provider : ServiceLoader.load(StreamCipherProvider.class)) {
            if (id.equalsIgnoreCase(provider.getId())) {
                return provider;
            }
        }
        return null;
    }
}
