/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

/**
 * {@linkplain ComparableBinding} for {@linkplain Integer} values.
 *
 * In addition to typical {@linkplain #intToEntry(int)} and {@linkplain #entryToInt(ByteIterable)} methods operating
 * with {@code ByteIterables} of length {@code 4}, {@code IntegerBinding} has a pair of methods for
 * serialization/deserialization of non-negative values to/from compressed entries:
 * {@linkplain #intToCompressedEntry(int)} and {@linkplain #compressedEntryToInt(ByteIterable)}. The lower the value,
 * the shorter the compressed entry. In some cases, compressed entries let you significantly decrease database size.
 * Serialization of non-negative integers and longs to compressed entries also saves the order of values.
 *
 * @see ComparableBinding
 */
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

    /**
     * De-serializes {@linkplain ByteIterable} entry to an {@code int} value.
     * The entry should an output of {@linkplain #intToEntry(int)}.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     * @see #intToEntry(int)
     * @see #compressedEntryToInt(ByteIterable)
     * @see #intToCompressedEntry(int)
     * @see #compressedEntryToSignedInt(ByteIterable)
     * @see #signedIntToCompressedEntry(int)
     */
    public static int entryToInt(@NotNull final ByteIterable entry) {
        return (Integer) BINDING.entryToObject(entry);
    }

    /**
     * Serializes {@code int} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     * @see #entryToInt(ByteIterable)
     * @see #compressedEntryToInt(ByteIterable)
     * @see #intToCompressedEntry(int)
     * @see #compressedEntryToSignedInt(ByteIterable)
     * @see #signedIntToCompressedEntry(int)
     */
    public static ArrayByteIterable intToEntry(final int object) {
        return BINDING.objectToEntry(object);
    }

    /**
     * De-serializes compressed {@linkplain ByteIterable} entry to an unsigned {@code int} value.
     * The entry should be an output of {@linkplain #intToCompressedEntry(int)}.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     * @see #entryToInt(ByteIterable)
     * @see #intToEntry(int)
     * @see #intToCompressedEntry(int)
     * @see #compressedEntryToSignedInt(ByteIterable)
     * @see #signedIntToCompressedEntry(int)
     */
    public static int compressedEntryToInt(@NotNull final ByteIterable entry) {
        return readCompressed(entry.iterator());
    }

    /**
     * Serializes unsigned {@code int} value to the compressed {@linkplain ArrayByteIterable} entry.
     *
     * @param object non-negative value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     * @see #entryToInt(ByteIterable)
     * @see #intToEntry(int)
     * @see #compressedEntryToInt(ByteIterable)
     * @see #compressedEntryToSignedInt(ByteIterable)
     * @see #signedIntToCompressedEntry(int)
     */
    public static ArrayByteIterable intToCompressedEntry(final int object) {
        if (object < 0) {
            throw new IllegalArgumentException();
        }
        final LightOutputStream output = new LightOutputStream(5);
        writeCompressed(output, object);
        return output.asArrayByteIterable();
    }

    /**
     * De-serializes compressed {@linkplain ByteIterable} entry to a signed {@code int} value.
     * The entry should be an output of {@linkplain #signedIntToCompressedEntry(int)}.
     * <p><a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a> is used,
     * so it doesn't save the order of values like other {@code ComparableBindings} do.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     * @see #entryToInt(ByteIterable)
     * @see #intToEntry(int)
     * @see #compressedEntryToInt(ByteIterable)
     * @see #intToCompressedEntry(int)
     * @see #signedIntToCompressedEntry(int)
     */
    public static int compressedEntryToSignedInt(@NotNull final ByteIterable entry) {
        final int result = compressedEntryToInt(entry);
        return (result >> 1) ^ (((result & 1) << 31) >> 31);
    }

    /**
     * Serializes signed {@code int} value in the range {@code [Integer.MIN_VALUE/2..Integer.MAX_VALUE/2]}
     * to the compressed {@linkplain ArrayByteIterable} entry.
     * <p><a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a> is used,
     * so it doesn't save the order of values like other {@code ComparableBindings} do.
     *
     * @param object value to serialize in the range {@code [Integer.MIN_VALUE/2..Integer.MAX_VALUE/2]}
     * @return {@linkplain ArrayByteIterable} entry
     * @see #entryToInt(ByteIterable)
     * @see #intToEntry(int)
     * @see #compressedEntryToInt(ByteIterable)
     * @see #intToCompressedEntry(int)
     * @see #compressedEntryToSignedInt(ByteIterable)
     */
    public static ArrayByteIterable signedIntToCompressedEntry(final int object) {
        return intToCompressedEntry((object << 1) ^ (object >> 31));
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

    public static int readCompressed(@NotNull final byte[] bytes) {
        final int firstByte = bytes[0] & 0xff;
        int result = firstByte & 0x1f;
        int byteLen = firstByte >> 5;
        for (int i = 1; i <= byteLen; ++i) {
            result = (result << 8) + (bytes[i] & 0xff);
        }
        return result;
    }

    public static void writeCompressed(@NotNull final LightOutputStream output, int i) {
        writeCompressed(output, i, new int[4]);
    }

    public static void writeCompressed(@NotNull final LightOutputStream output, int i, final int[] bytes) {
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
