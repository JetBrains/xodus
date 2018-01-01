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
 * {@linkplain ComparableBinding} for {@linkplain Boolean} values.
 *
 * @see ComparableBinding
 */
public final class BooleanBinding extends ComparableBinding {

    public static final BooleanBinding BINDING = new BooleanBinding();

    private BooleanBinding() {
    }

    @Override
    public Boolean readObject(@NotNull final ByteArrayInputStream stream) {
        return stream.read() != 0;
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        output.write(((Boolean) object) ? 1 : 0);
    }

    /**
     * De-serializes {@linkplain ByteIterable} entry to a {@code boolean} value.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     */
    public static boolean entryToBoolean(@NotNull final ByteIterable entry) {
        return (Boolean) BINDING.entryToObject(entry);
    }

    /**
     * Serializes {@code boolean} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     */
    public static ArrayByteIterable booleanToEntry(final boolean object) {
        return BINDING.objectToEntry(object);
    }
}
