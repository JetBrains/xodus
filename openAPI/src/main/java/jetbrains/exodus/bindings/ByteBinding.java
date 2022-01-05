/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.bindings;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

/**
 * {@linkplain ComparableBinding} for {@linkplain Byte} values.
 *
 * @see ComparableBinding
 */
public final class ByteBinding extends ComparableBinding {

    public static final ByteBinding BINDING = new ByteBinding();

    private ByteBinding() {
    }

    @Override
    public Byte readObject(@NotNull final ByteArrayInputStream stream) {
        return (byte) (stream.read() ^ 0x80);
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        output.writeByte((Byte) object);
    }

    /**
     * De-serializes {@linkplain ByteIterable} entry to a {@code byte} value.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     */
    public static byte entryToByte(@NotNull final ByteIterable entry) {
        return (Byte) BINDING.entryToObject(entry);
    }

    /**
     * Serializes {@code byte} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     */
    public static ArrayByteIterable byteToEntry(final byte object) {
        return BINDING.objectToEntry(object);
    }
}
