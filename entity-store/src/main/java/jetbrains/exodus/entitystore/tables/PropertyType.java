/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.bindings.*;
import org.jetbrains.annotations.NotNull;

public class PropertyType {

    public static final int STRING_PROPERTY_TYPE = 2;

    static final PropertyType[] PREDEFINED_TYPES;

    static {
        PREDEFINED_TYPES = new PropertyType[8];
        PREDEFINED_TYPES[0] = new PropertyType(0, IntegerBinding.BINDING, Integer.class);
        PREDEFINED_TYPES[1] = new PropertyType(1, LongBinding.BINDING, Long.class);
        PREDEFINED_TYPES[STRING_PROPERTY_TYPE] = new PropertyType(STRING_PROPERTY_TYPE, StringBinding.BINDING, String.class);
        PREDEFINED_TYPES[3] = new PropertyType(3, DoubleBinding.BINDING, Double.class);
        PREDEFINED_TYPES[4] = new PropertyType(4, ByteBinding.BINDING, Byte.class);
        PREDEFINED_TYPES[5] = new PropertyType(5, BooleanBinding.BINDING, Boolean.class);
        PREDEFINED_TYPES[6] = new PropertyType(6, ShortBinding.BINDING, Short.class);
        PREDEFINED_TYPES[7] = new PropertyType(7, FloatBinding.BINDING, Float.class);
    }

    private final int typeId;
    private final ComparableBinding binding;
    private final Class<? extends Comparable> clazz;

    PropertyType(final int typeId, @NotNull final ComparableBinding binding, @NotNull final Class<? extends Comparable> clazz) {
        this.typeId = typeId;
        this.binding = binding;
        this.clazz = clazz;
    }

    public int getTypeId() {
        return typeId;
    }

    public ComparableBinding getBinding() {
        return binding;
    }

    public Class<? extends Comparable> getClazz() {
        return clazz;
    }
}
