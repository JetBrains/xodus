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
package jetbrains.exodus.bindings;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

/**
 * {@linkplain ComparableBinding} for {@linkplain Long} values.
 *
 * In addition to typical {@linkplain #longToEntry(long)} and {@linkplain #entryToLong(ByteIterable)} methods operating
 * with {@code ByteIterables} of length {@code 8}, {@code LongBinding} has a pair of methods for
 * serialization/deserialization of non-negative values to/from compressed entries:
 * {@linkplain #longToCompressedEntry(long)} and {@linkplain #compressedEntryToLong(ByteIterable)}. The lower the value,
 * the shorter the compressed entry. In some cases, compressed entries let you significantly decrease database size.
 * Serialization of non-negative integers and longs to compressed entries also saves the order of values.
 *
 * @see ComparableBinding
 */
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

    /**
     * De-serializes {@linkplain ByteIterable} entry to a {@code long} value.
     * The entry should be an output of {@linkplain #longToEntry(long)}.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     * @see #longToEntry(long)
     * @see #compressedEntryToLong(ByteIterable)
     * @see #longToCompressedEntry(long)
     * @see #compressedEntryToSignedLong(ByteIterable)
     * @see #signedLongToCompressedEntry(long)
     */
    public static long entryToLong(@NotNull final ByteIterable entry) {
        return (Long) BINDING.entryToObject(entry);
    }

    /**
     * Serializes {@code long} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     * @see #entryToLong(ByteIterable)
     * @see #compressedEntryToLong(ByteIterable)
     * @see #longToCompressedEntry(long)
     * @see #compressedEntryToSignedLong(ByteIterable)
     * @see #signedLongToCompressedEntry(long)
     */
    public static ArrayByteIterable longToEntry(final long object) {
        return BINDING.objectToEntry(object);
    }

    /**
     * De-serializes compressed {@linkplain ByteIterable} entry to an unsigned {@code long} value.
     * The entry should be an output of {@linkplain #longToCompressedEntry(long)}.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     * @see #entryToLong(ByteIterable)
     * @see #longToEntry(long)
     * @see #longToCompressedEntry(long)
     * @see #compressedEntryToSignedLong(ByteIterable)
     * @see #signedLongToCompressedEntry(long)
     */
    public static long compressedEntryToLong(@NotNull final ByteIterable entry) {
        return readCompressed(entry.iterator());
    }

    /**
     * Serializes unsigned {@code long} value to the compressed {@linkplain ArrayByteIterable} entry.
     *
     * @param object non-negative value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     * @see #entryToLong(ByteIterable)
     * @see #longToEntry(long)
     * @see #compressedEntryToLong(ByteIterable)
     * @see #compressedEntryToSignedLong(ByteIterable)
     * @see #signedLongToCompressedEntry(long)
     */
    public static ArrayByteIterable longToCompressedEntry(final long object) {
        if (object < 0) {
            throw new IllegalArgumentException();
        }
        final LightOutputStream output = new LightOutputStream(7);
        writeCompressed(output, object);
        return output.asArrayByteIterable();
    }

    /**
     * De-serializes compressed {@linkplain ByteIterable} entry to a signed {@code long} value.
     * The entry should be an output of {@linkplain #signedLongToCompressedEntry(long)}.
     * <p><a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a> is used,
     * so it doesn't save the order of values like other {@code ComparableBindings} do.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     * @see #entryToLong(ByteIterable)
     * @see #longToEntry(long)
     * @see #compressedEntryToLong(ByteIterable)
     * @see #longToCompressedEntry(long)
     * @see #signedLongToCompressedEntry(long)
     */
    public static long compressedEntryToSignedLong(@NotNull final ByteIterable entry) {
        final long result = compressedEntryToLong(entry);
        return (result >> 1) ^ (((result & 1) << 63) >> 63);
    }

    /**
     * Serializes signed {@code long} value in the range {@code [Long.MIN_VALUE/2..Long.MAX_VALUE/2]}
     * to the compressed {@linkplain ArrayByteIterable} entry.
     * <p><a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a> is used,
     * so it doesn't save the order of values like other {@code ComparableBindings} do.
     *
     * @param object value to serialize in the range {@code [Long.MIN_VALUE/2..Long.MAX_VALUE/2]}
     * @return {@linkplain ArrayByteIterable} entry
     * @see #entryToLong(ByteIterable)
     * @see #longToEntry(long)
     * @see #compressedEntryToLong(ByteIterable)
     * @see #longToCompressedEntry(long)
     * @see #compressedEntryToSignedLong(ByteIterable)
     */
    public static ArrayByteIterable signedLongToCompressedEntry(final long object) {
        return longToCompressedEntry((object << 1) ^ (object >> 63));
    }

    public static void writeUnsignedLong(final long l,
                                         final int bytesPerLong,
                                         @NotNull final LightOutputStream output) {
        int bits = bytesPerLong << 3;
        while (bits > 0) {
            output.write((int) (l >> (bits -= 8) & 0xff));
        }
    }

    public static long entryToUnsignedLong(@NotNull final ByteIterator bi, final int length) {
        long result = 0;
        for (int i = 0; i < length; ++i) {
            result = (result << 8) + ((int) bi.next() & 0xff);
        }
        return result;
    }

    public static long entryToUnsignedLong(@NotNull final byte[] bytes, final int offset, final int length) {
        long result = 0;
        for (int i = 0; i < length; ++i) {
            result = (result << 8) + ((int) bytes[offset + i] & 0xff);
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
        writeCompressed(output, l, new int[8]);
    }

    public static void writeCompressed(@NotNull final LightOutputStream output, long l, final int[] bytes) {
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
