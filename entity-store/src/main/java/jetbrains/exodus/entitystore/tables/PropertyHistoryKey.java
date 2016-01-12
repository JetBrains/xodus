/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

public class PropertyHistoryKey {

    private final long entityLocalId;
    private final int version;
    private final int propertyId;

    public PropertyHistoryKey(final long entityLocalId, final int version, final int propertyId) {
        this.entityLocalId = entityLocalId;
        this.version = version;
        this.propertyId = propertyId;
    }

    public long getEntityLocalId() {
        return entityLocalId;
    }

    public int getVersion() {
        return version;
    }

    public int getPropertyId() {
        return propertyId;
    }

    public static PropertyHistoryKey entryToPropertyHistoryKey(@NotNull final ByteIterable entry) {
        final ByteIterator it = entry.iterator();
        final long entityLocalId = LongBinding.readCompressed(it);
        final int version = IntegerBinding.readCompressed(it);
        final int propertyId = IntegerBinding.readCompressed(it);
        return new PropertyHistoryKey(entityLocalId, version, propertyId);
    }

    public static ArrayByteIterable propertyHistoryKeyToEntry(@NotNull final PropertyHistoryKey object) {
        final LightOutputStream output = new LightOutputStream();
        LongBinding.writeCompressed(output, object.entityLocalId);
        IntegerBinding.writeCompressed(output, object.version);
        IntegerBinding.writeCompressed(output, object.propertyId);
        return output.asArrayByteIterable();
    }
}
