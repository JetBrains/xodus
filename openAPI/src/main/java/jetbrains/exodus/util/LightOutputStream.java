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

import jetbrains.exodus.ArrayByteIterable;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.Arrays;

public class LightOutputStream extends OutputStream {

    private static final int DEFAULT_INIT_SIZE = 10;

    private int len;
    private byte[] buf;

    public LightOutputStream() {
        this(DEFAULT_INIT_SIZE);
    }

    public LightOutputStream(int initialSize) {
        this(new byte[initialSize]);
    }

    public LightOutputStream(byte[] buffer) {
        buf = buffer;
        len = 0;
    }

    @NotNull
    public ArrayByteIterable asArrayByteIterable() {
        return new ArrayByteIterable(buf, len);
    }

    public void clear() {
        len = 0;
    }

    public int size() {
        return len;
    }

    @Override
    public void write(int b) {
        if (len >= buf.length) {
            ensureCapacity(len + 1);
        }
        buf[len++] = (byte) b;
    }

    @Override
    public void write(@NotNull byte[] fromBuf) {
        ensureCapacity(len + fromBuf.length);
        System.arraycopy(fromBuf, 0, buf, len, fromBuf.length);
        len += fromBuf.length;
    }

    @Override
    public void write(@NotNull byte[] fromBuf, int offset, int length) {
        ensureCapacity(len + length);
        System.arraycopy(fromBuf, offset, buf, len, length);
        len += length;
    }

    @Override
    public String toString() {
        return new String(buf, 0, len);
    }

    public final void writeByte(int val) {
        write(val ^ 0x80);
    }

    public final void writeString(final String value) {
        if (value == null) {
            write(UTFUtil.NULL_STRING_UTF_VALUE);
        } else if (value.length() > 0) {
            final int utfLength = UTFUtil.getUtfByteLength(value);
            ensureCapacity(len + utfLength + 1); // + 1 for zero terminator
            UTFUtil.utfCharsToBytes(value, buf, len);
            len += utfLength;
        }
        write(0);
    }

    public final void writeUnsignedShort(int val) {
        write((byte) (val >>> 8));
        write((byte) val);
    }

    public final void writeUnsignedInt(long val) {
        write((byte) (val >>> 24));
        write((byte) (val >>> 16));
        write((byte) (val >>> 8));
        write((byte) val);
    }

    public final void writeUnsignedLong(long val) {
        write((byte) (val >>> 56));
        write((byte) (val >>> 48));
        write((byte) (val >>> 40));
        write((byte) (val >>> 32));
        write((byte) (val >>> 24));
        write((byte) (val >>> 16));
        write((byte) (val >>> 8));
        write((byte) val);
    }

    public byte[] getBufferBytes() {
        return buf;
    }

    private void ensureCapacity(int requiredCapacity) {
        final int bufLen = buf.length;
        if (bufLen < requiredCapacity) {
            buf = Arrays.copyOf(buf, Math.max(requiredCapacity,
                    bufLen < 50 ? bufLen << 2 : (bufLen < 1000 ? bufLen << 1 : (bufLen << 3) / 5)));
        }
    }
}
