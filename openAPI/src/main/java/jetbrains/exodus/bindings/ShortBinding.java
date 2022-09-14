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
 * {@linkplain ComparableBinding} for {@linkplain Short} values.
 *
 * @see ComparableBinding
 */
public class ShortBinding extends ComparableBinding {

    public static final ShortBinding BINDING = new ShortBinding();

    private ShortBinding() {
    }

    @Override
    public Short readObject(@NotNull final ByteArrayInputStream stream) {
        return BindingUtils.readShort(stream);
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        output.writeUnsignedShort((int) (Short) object ^ 0x8000);
    }

    /**
     * De-serializes {@linkplain ByteIterable} entry to a {@code short} value.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     */
    public static short entryToShort(@NotNull final ByteIterable entry) {
        return entry.getShort(0);
    }

    /**
     * Serializes {@code short} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     */
    public static ArrayByteIterable shortToEntry(final short object) {
        return BINDING.objectToEntry(object);
    }
}
