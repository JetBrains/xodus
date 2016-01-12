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

public class IntegerBinding extends ComparableBinding {

    public static final IntegerBinding BINDING = new IntegerBinding();

    private IntegerBinding() {
    }

    @Override
    public Integer readObject(@NotNull final ByteArrayInputStream stream) {
        return BindingUtils.readInt(stream);
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        output.writeUnsignedInt((Integer) object ^ 0x80000000);
    }

    public static int entryToInt(@NotNull final ByteIterable entry) {
        return (Integer) BINDING.entryToObject(entry);
    }

    public static ArrayByteIterable intToEntry(final int object) {
        return BINDING.objectToEntry(object);
    }

    public static int compressedEntryToInt(@NotNull final ByteIterable entry) {
        return readCompressed(entry.iterator());
    }

    public static ArrayByteIterable intToCompressedEntry(final int object) {
        final LightOutputStream output = new LightOutputStream(5);
        writeCompressed(output, object);
        return output.asArrayByteIterable();
    }

    public static int readCompressed(@NotNull final ByteIterator iterator) {
        final int firstByte = iterator.next() & 0xff;
        int result = firstByte & 0x1f;
        int byteLen = firstByte >> 5;
        while (--byteLen >= 0) {
            result = (result << 8) + (iterator.next() & 0xff);
        }
        return result;
    }

    public static void writeCompressed(@NotNull final LightOutputStream output, int i) {
        final int[] bytes = new int[4];
        for (int j = 0; j < 4; ++j) {
            bytes[j] = i & 0xff;
            i >>= 8;
        }
        int byteLen = 4;
        while (byteLen > 0 && bytes[byteLen - 1] == 0) {
            --byteLen;
        }
        int firstByte = byteLen << 5;
        if (byteLen > 0) {
            final int upperByte = bytes[byteLen - 1];
            if (upperByte < 32) {
                firstByte = (((firstByte >> 5) - 1) << 5) + upperByte;
                --byteLen;
            }
        }
        output.write(firstByte);
        while (--byteLen >= 0) {
            output.write(bytes[byteLen]);
        }
    }
}
