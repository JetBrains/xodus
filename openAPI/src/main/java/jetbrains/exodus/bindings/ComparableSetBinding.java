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
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

/**
 * {@linkplain ComparableBinding} for {@linkplain ComparableSet} values.
 *
 * @see ComparableBinding
 * @see ComparableSet
 */
@SuppressWarnings("unchecked")
public class ComparableSetBinding extends ComparableBinding {

    public static final ComparableSetBinding BINDING = new ComparableSetBinding();

    private ComparableSetBinding() {
    }

    @Override
    public ComparableSet readObject(@NotNull final ByteArrayInputStream stream) {
        final int valueTypeId = stream.read() ^ 0x80;
        final ComparableBinding itemBinding = ComparableValueType.getPredefinedBinding(valueTypeId);
        final ComparableSet result = new ComparableSet();
        while (stream.available() > 0) {
            result.addItem(itemBinding.readObject(stream));
        }
        result.setIsDirty(false);
        return result;
    }

    @Override
    public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
        final ComparableSet set = (ComparableSet) object;
        final Class itemClass = set.getItemClass();
        if (itemClass == null) {
            throw new ExodusException("Attempt to write empty ComparableSet");
        }
        final ComparableValueType type = ComparableValueType.getPredefinedType(itemClass);
        output.writeByte(type.getTypeId());
        final ComparableBinding itemBinding = type.getBinding();
        //noinspection Convert2Lambda
        set.forEach(new ComparableSet.Consumer() {
            @Override
            public void accept(@NotNull final Comparable item, final int index) {
                itemBinding.writeObject(output, item);
            }
        });
    }

    /**
     * De-serializes {@linkplain ByteIterable} entry to a {@code ComparableSet} value.
     *
     * @param entry {@linkplain ByteIterable} instance
     * @return de-serialized value
     */
    public static ComparableSet entryToComparableSet(@NotNull final ByteIterable entry) {
        return (ComparableSet) BINDING.entryToObject(entry);
    }

    /**
     * Serializes {@code ComparableSet} value to the {@linkplain ArrayByteIterable} entry.
     *
     * @param object value to serialize
     * @return {@linkplain ArrayByteIterable} entry
     */
    public static ArrayByteIterable comparableSetToEntry(@NotNull final ComparableSet object) {
        return BINDING.objectToEntry(object);
    }
}
