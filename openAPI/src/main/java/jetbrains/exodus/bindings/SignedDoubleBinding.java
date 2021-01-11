/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

/**
 * {@linkplain ComparableBinding} for signed {@linkplain Double} values.
 * For unsigned non-negative values, {@linkplain DoubleBinding} can be used.
 *
 * @see DoubleBinding
 * @see ComparableBinding
 */
public class SignedDoubleBinding extends ComparableBinding {

    public static final SignedDoubleBinding BINDING = new SignedDoubleBinding();

    private SignedDoubleBinding() {
    }

    @Override
    public Double readObject(@NotNull final ByteArrayInputStream stream) {
        return BindingUtils.readSignedDouble(stream);
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        final long longValue = Double.doubleToLongBits((Double) object);
        output.writeUnsignedLong(longValue ^ (longValue < 0 ? 0xffffffffffffffffL : 0x8000000000000000L));
    }

    /**
     * De-serializes {@linkplain ByteIterable} entry to a {@code double} value.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     */
    public static double entryToDouble(@NotNull final ByteIterable entry) {
        return (Double) BINDING.entryToObject(entry);
    }

    /**
     * Serializes {@code double} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     */
    public static ArrayByteIterable doubleToEntry(final double object) {
        return BINDING.objectToEntry(object);
    }
}

