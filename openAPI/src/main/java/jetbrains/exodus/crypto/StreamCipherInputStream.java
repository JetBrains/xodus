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
package jetbrains.exodus.crypto;

import org.jetbrains.annotations.NotNull;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public class StreamCipherInputStream extends FilterInputStream {
    private StreamCipher cipher;
    private final Supplier<StreamCipher> cipherSupplier;

    private int position;
    private int savedPosition;

    public StreamCipherInputStream(InputStream in, Supplier<StreamCipher> cipherSupplier) {
        super(in);

        this.cipherSupplier = cipherSupplier;
        cipher = cipherSupplier.get();
    }

    @Override
    public int read() throws IOException {
        var read = super.read();
        if (read != -1) {
            position++;

            return cipher.crypt((byte) read) & 0xFF;

        }

        return -1;
    }

    @Override
    public int read(byte @NotNull [] b) throws IOException {
        var read = super.read(b);

        if (read != -1) {
            for (int i = 0; i < read; ++i) {
                b[i] = cipher.crypt(b[i]);
            }

            position += read;
        }

        return read;
    }

    @Override
    public int read(byte @NotNull [] b, int off, int len) throws IOException {
        var read = super.read(b, off, len);
        if (read != -1) {
            for (int i = 0; i < read; ++i) {
                b[off + i] = cipher.crypt(b[off + i]);
            }

            position += read;
        }

        return read;
    }

    @Override
    public synchronized void reset() throws IOException {
        super.reset();
        cipher = cipherSupplier.get();

        for (int i = 0; i < savedPosition; ++i) {
            cipher.crypt((byte) 0);
        }

        position = savedPosition;
    }

    @Override
    public synchronized void mark(int readlimit) {
        super.mark(readlimit);
        savedPosition = position;
    }
}
