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
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

/**
 * {@linkplain ComparableBinding} for {@linkplain Float} values.
 *
 * @see ComparableBinding
 */
public class FloatBinding extends ComparableBinding {

    public static final FloatBinding BINDING = new FloatBinding();

    private FloatBinding() {
    }

    @Override
    public Float readObject(@NotNull final ByteArrayInputStream stream) {
        return BindingUtils.readFloat(stream);
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        output.writeUnsignedInt(Float.floatToIntBits((Float) object));
    }

    /**
     * De-serializes {@linkplain ByteIterable} entry to a {@code float} value.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     */
    public static float entryToFloat(@NotNull final ByteIterable entry) {
        return (Float) BINDING.entryToObject(entry);
    }

    /**
     * Serializes {@code float} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     */
    public static ArrayByteIterable floatToEntry(final float object) {
        return BINDING.objectToEntry(object);
    }
}
