/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public final class PropertyKey {

    private final long entityLocalId;
    private final int propertyId;

    public PropertyKey(final long entityLocalId, final int propertyId) {
        this.entityLocalId = entityLocalId;
        this.propertyId = propertyId;
    }

    public long getEntityLocalId() {
        return entityLocalId;
    }

    public int getPropertyId() {
        return propertyId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyKey that = (PropertyKey) o;
        return entityLocalId == that.entityLocalId && propertyId == that.propertyId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityLocalId, propertyId);
    }

    public static PropertyKey entryToPropertyKey(@NotNull final ByteIterable entry) {
        final ByteIterator it = entry.iterator();
        final long entityLocalId = LongBinding.readCompressed(it);
        final int propertyId = IntegerBinding.readCompressed(it);
        return new PropertyKey(entityLocalId, propertyId);
    }

    public static ArrayByteIterable propertyKeyToEntry(@NotNull final PropertyKey object) {
        return propertyKeyToEntry(new LightOutputStream(7), new int[8], object.entityLocalId, object.propertyId);
    }

    public static ArrayByteIterable propertyKeyToEntry(final LightOutputStream output,
                                                       @NotNull final int[] bytes,
                                                       final long localId,
                                                       final int propertyId) {
        output.clear();
        writePropertyKey(output, bytes, localId, propertyId);
        return output.asArrayByteIterable();
    }

    private static void writePropertyKey(@NotNull final LightOutputStream output,
                                         @NotNull final int[] bytes,
                                         final long localId,
                                         final int propertyId) {
        LongBinding.writeCompressed(output, localId, bytes);
        IntegerBinding.writeCompressed(output, propertyId, bytes);
    }
}
