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
package jetbrains.exodus.util;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Lightweight substitute of ByteArrayOutputStream: toByteArray() doesn't create a copy.
 */
public class LightByteArrayOutputStream extends ByteArrayOutputStream {

    public LightByteArrayOutputStream() {
        super();
    }

    public LightByteArrayOutputStream(final int size) {
        super(size);
    }

    public int write(@NotNull final ByteBuffer buffer) {
        final int bufferLen = buffer.remaining();
        if (bufferLen > 0) {
            final int newCount = count + bufferLen;
            if (newCount > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newCount));
            }
            buffer.get(buf, count, bufferLen);
            count = newCount;
        }
        return bufferLen;
    }

    @Override
    public byte toByteArray()[] {
        return buf;
    }

    @Override
    public int size() {
        return count;
    }

    public void setSize(final int size) {
        count = size;
    }
}
