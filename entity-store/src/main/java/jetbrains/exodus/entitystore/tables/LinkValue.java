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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

public class LinkValue {

    @NotNull
    private final EntityId entityId;
    private final int linkId;

    public LinkValue(@NotNull final EntityId entityId, final int linkId) {
        this.entityId = entityId;
        this.linkId = linkId;
    }

    @NotNull
    public EntityId getEntityId() {
        return entityId;
    }

    public int getLinkId() {
        return linkId;
    }

    public static LinkValue entryToLinkValue(@NotNull final ByteIterable entry) {
        final ByteIterator it = entry.iterator();
        final int linkId = IntegerBinding.readCompressed(it);
        return new LinkValue(EntityIdBinding.iteratorToEntityId(it), linkId);
    }

    public static ArrayByteIterable linkValueToEntry(@NotNull final LinkValue object) {
        final LightOutputStream output = new LightOutputStream();
        final int[] bytes = new int[8];
        IntegerBinding.writeCompressed(output, object.linkId, bytes);
        EntityIdBinding.writeEntityId(output, object.entityId, bytes);
        return output.asArrayByteIterable();
    }
}
