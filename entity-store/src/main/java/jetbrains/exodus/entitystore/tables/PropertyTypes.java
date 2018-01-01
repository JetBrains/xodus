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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.bindings.ComparableValueType;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityStoreException;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;

public final class PropertyTypes {

    private final IntHashMap<ComparableValueType> typesById;
    private final HashMap<Class<? extends Comparable>, ComparableValueType> typesByClass;

    public PropertyTypes() {
        typesById = new IntHashMap<>();
        typesByClass = new HashMap<>();
        clear();
    }

    public void clear() {
        typesById.clear();
        typesByClass.clear();
        for (final ComparableValueType predefinedType : ComparableValueType.PREDEFINED_COMPARABLE_VALUE_TYPES) {
            typesById.put(predefinedType.getTypeId(), predefinedType);
            typesByClass.put(predefinedType.getClazz(), predefinedType);
        }
    }

    @NotNull
    public ComparableValueType getPropertyType(final int typeId) {
        final ComparableValueType result = typesById.get(typeId);
        if (result == null) {
            throw new EntityStoreException("Unsupported property type id " + typeId);
        }
        return result;
    }

    @NotNull
    public ComparableValueType getPropertyType(@NotNull final Class<? extends Comparable> clazz) {
        final ComparableValueType result = typesByClass.get(clazz);
        if (result == null) {
            throw new EntityStoreException("Unsupported property type " + clazz);
        }
        return result;
    }

    public void registerCustomPropertyType(int typeId,
                                           @NotNull final Class<? extends Comparable> clazz,
                                           @NotNull final ComparableBinding binding) {
        typeId += ComparableValueType.PREDEFINED_COMPARABLE_VALUE_TYPES.length;
        final ComparableValueType propType = new ComparableValueType(typeId, binding, clazz);
        if (typesById.put(typeId, propType) != null) {
            throw new EntityStoreException("Already registered property type id " + typeId);
        }
        if (typesByClass.put(clazz, propType) != null) {
            throw new EntityStoreException("Already registered property type " + clazz);
        }
    }

    public PropertyValue entryToPropertyValue(@NotNull final ByteIterable entry) {
        final ByteIterableBase it = (ByteIterableBase) entry;
        final byte[] bytes = it.getBytesUnsafe();
        final ComparableValueType type = getPropertyType((byte) (bytes[0] ^ 0x80));
        final Comparable data = type.getBinding().readObject(new ByteArrayInputStream(bytes, 1, it.getLength() - 1));
        return new PropertyValue(type, data);
    }

    public PropertyValue dataToPropertyValue(@NotNull final Comparable data) {
        return new PropertyValue(getPropertyType(data.getClass()), data);
    }

    public ArrayByteIterable dataArrayToEntry(@NotNull final Comparable[] dataArray) {
        final LightOutputStream out = new LightOutputStream();
        for (final Comparable data : dataArray) {
            if (data instanceof Entity) {
                EntityIdBinding.objectToEntry(((Entity) data).getId()).writeTo(out);
            } else {
                writePropertyValue(out, dataToPropertyValue(toLowerCase(data)));
            }
        }
        return out.asArrayByteIterable();
    }

    public static ArrayByteIterable propertyValueToEntry(@NotNull final PropertyValue object) {
        final LightOutputStream output = new LightOutputStream();
        writePropertyValue(output, object);
        return output.asArrayByteIterable();
    }

    public static Comparable toLowerCase(@Nullable final Comparable value) {
        if (!(value instanceof String)) return value;
        return ((String) value).toLowerCase();
    }

    private static void writePropertyValue(@NotNull final LightOutputStream output, @NotNull final PropertyValue object) {
        output.write(object.getType().getTypeId() ^ 0x80);
        object.getBinding().writeObject(output, object.getData());
    }
}
