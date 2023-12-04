/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

/**
 * {@linkplain ComparableBinding} for unsigned non-negative {@linkplain Double} values.
 * For signed values use {@linkplain SignedDoubleBinding}.
 *
 * @see SignedDoubleBinding
 * @see ComparableBinding
 */
public final class DoubleBinding extends ComparableBinding {

    public static final DoubleBinding BINDING = new DoubleBinding();

    private DoubleBinding() {
    }

    @Override
    public Double readObject(@NotNull final ByteArrayInputStream stream) {
        return BindingUtils.readUnsignedDouble(stream);
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        final double value = (Double) object;
        if (value < 0) {
            throw new ExodusException("DoubleBinding can be used only for unsigned non-negative values.");
        }
        output.writeUnsignedLong(Double.doubleToLongBits(value));
    }

    /**
     * De-serializes {@linkplain ByteIterable} entry to an unsigned non-negative {@code double} value.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     */
    public static double entryToDouble(@NotNull final ByteIterable entry) {
        return (Double) BINDING.entryToObject(entry);
    }

    /**
     * Serializes unsigned non-negative {@code double} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     */
    public static ArrayByteIterable doubleToEntry(final double object) {
        return BINDING.objectToEntry(object);
    }
}
