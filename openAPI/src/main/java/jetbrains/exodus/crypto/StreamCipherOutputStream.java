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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class StreamCipherOutputStream extends FilterOutputStream {
    private final StreamCipher cipher;

    public StreamCipherOutputStream(OutputStream out, StreamCipher cipher) {
        super(out);
        this.cipher = cipher;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(cipher.crypt((byte) b) & 0xff);
    }

    @Override
    public void write(byte @NotNull [] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte @NotNull [] b, int off, int len) throws IOException {
        var encrypted = new byte[len];

        for (int i = 0; i < len; i++) {
            encrypted[i] = cipher.crypt(b[i + off]);
        }

        out.write(encrypted, 0, len);
    }
}
