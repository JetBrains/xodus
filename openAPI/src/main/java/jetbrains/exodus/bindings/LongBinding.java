/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.bindings;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

public class LongBinding extends ComparableBinding {

    public static final LongBinding BINDING = new LongBinding();

    private LongBinding() {
    }

    @Override
    public Long readObject(@NotNull final ByteArrayInputStream stream) {
        return BindingUtils.readLong(stream);
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        output.writeUnsignedLong((Long) object ^ 0x8000000000000000L);
    }

    public static long entryToLong(@NotNull final ByteIterable entry) {
        return (Long) BINDING.entryToObject(entry);
    }

    public static ArrayByteIterable longToEntry(final long object) {
        return BINDING.objectToEntry(object);
    }

    public static long compressedEntryToLong(@NotNull final ByteIterable bi) {
        return readCompressed(bi.iterator());
    }

    public static ArrayByteIterable longToCompressedEntry(final long object) {
        final LightOutputStream output = new LightOutputStream(7);
        writeCompressed(output, object);
        return output.asArrayByteIterable();
    }

    public static void writeUnsignedLong(final long l,
                                         final int bytesPerLong,
                                         @NotNull final LightOutputStream output) {
        int bits = bytesPerLong << 3;
        while (bits > 0) {
            output.write((int) (l >> (bits -= 8) & 0xff));
        }
    }

    public static long entryToUnsignedLong(@NotNull final ByteIterator bi, final int bytesPerLong) {
        long result = 0;
        for (int j = 0; j < bytesPerLong; ++j) {
            result = (result << 8) + ((int) bi.next() & 0xff);
        }
        return result;
    }

    public static long readCompressed(@NotNull final ByteIterator iterator) {
        final int firstByte = iterator.next() & 0xff;
        long result = firstByte & 0xf;
        int byteLen = firstByte >> 4;
        while (--byteLen >= 0) {
            result = (result << 8) + (iterator.next() & 0xff);
        }
        return result;
    }

    public static void writeCompressed(@NotNull final LightOutputStream output, long l) {
        final int[] bytes = new int[8];
        for (int i = 0; i < 8; ++i) {
            bytes[i] = (int) (l & 0xff);
            l >>= 8;
        }
        int byteLen = 8;
        while (byteLen > 0 && bytes[byteLen - 1] == 0) {
            --byteLen;
        }
        int firstByte = byteLen << 4;
        if (byteLen > 0) {
            final int upperByte = bytes[byteLen - 1];
            if (upperByte < 16) {
                firstByte = (((firstByte >> 4) - 1) << 4) + upperByte;
                --byteLen;
            }
        }
        output.write(firstByte);
        while (--byteLen >= 0) {
            output.write(bytes[byteLen]);
        }
    }
}
