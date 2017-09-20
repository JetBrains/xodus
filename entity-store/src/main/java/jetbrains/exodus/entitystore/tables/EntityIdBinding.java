/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

public class EntityIdBinding {

    @NotNull
    private final EntityId entityId;

    public EntityIdBinding(@NotNull final EntityId entityId) {
        this.entityId = entityId;
    }

    @NotNull
    public EntityId getEntityId() {
        return entityId;
    }

    public static EntityId entryToEntityId(@NotNull final ByteIterable entry) {
        final ByteIterator it = entry.iterator();
        return iteratorToEntityId(it);
    }

    public static ArrayByteIterable objectToEntry(@NotNull final EntityId object) {
        final LightOutputStream output = new LightOutputStream(7);
        writeEntityId(output, object);
        return output.asArrayByteIterable();
    }

    public static EntityId iteratorToEntityId(@NotNull final ByteIterator it) {
        final int entityTypeId = IntegerBinding.readCompressed(it);
        final long entityLocalId = LongBinding.readCompressed(it);
        return new PersistentEntityId(entityTypeId, entityLocalId);
    }

    public static void writeEntityId(@NotNull final LightOutputStream output, @NotNull final EntityId object) {
        writeEntityId(output, object, new int[8]);
    }

    public static void writeEntityId(@NotNull final LightOutputStream output, @NotNull final EntityId object, final int[] bytes) {
        IntegerBinding.writeCompressed(output, object.getTypeId(), bytes);
        LongBinding.writeCompressed(output, object.getLocalId(), bytes);
    }
}
