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
package jetbrains.exodus.benchmark.crypto;

import jetbrains.exodus.crypto.KryptKt;
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.crypto.StreamCipherProvider;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.crypto.streamciphers.ChaChaStreamCipherProviderKt.CHACHA_CIPHER_ID;
import static jetbrains.exodus.crypto.streamciphers.Salsa20StreamCipherProviderKt.SALSA20_CIPHER_ID;

@State(Scope.Thread)
public class JMHStreamCipherBenchmarks {

    private static final byte[] KEY_256_BITS;
    private static long IV;

    static {
        KEY_256_BITS = new byte[32];
        final Random rnd = new Random();
        rnd.nextBytes(KEY_256_BITS);
        IV = rnd.nextLong();
    }

    @NotNull
    private final StreamCipherProvider salsa20Provider = KryptKt.newCipherProvider(SALSA20_CIPHER_ID);
    private final StreamCipherProvider chaChaProvider = KryptKt.newCipherProvider(CHACHA_CIPHER_ID);
    private StreamCipher salsa20Cipher = salsa20Provider.newCipher();
    private StreamCipher chaChaCipher = chaChaProvider.newCipher();

    @Setup
    public void prepare() {
        salsa20Cipher.init(KEY_256_BITS, IV++);
        chaChaCipher.init(KEY_256_BITS, IV++);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(2)
    public Object getSalsa20Provider() {
        return KryptKt.newCipherProvider(SALSA20_CIPHER_ID);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(2)
    public Object getChaChaProvider() {
        return KryptKt.newCipherProvider(SALSA20_CIPHER_ID);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(2)
    public Object initSalsa20Cipher() {
        salsa20Cipher.init(KEY_256_BITS, IV++);
        return salsa20Cipher;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(2)
    public Object initChaChaCipher() {
        chaChaCipher.init(KEY_256_BITS, IV++);
        return chaChaCipher;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(2)
    public Object salsa20Crypt() {
        return salsa20Cipher.crypt((byte) 0);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(2)
    public Object chaChaCrypt() {
        return chaChaCipher.crypt((byte) 0);
    }
}
