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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.entitystore.iterate.EntityIterableHandleBase;
import org.jetbrains.annotations.NotNull;

public class PersistentEntityId implements EntityId {

    @NotNull
    public static final PersistentEntityId EMPTY_ID = new PersistentEntityId(0, 0);

    private static final long serialVersionUID = -3875948066835180514L;

    private final int entityTypeId;
    private final long entityLocalId;

    /**
     * Generic entity id constructor.
     *
     * @param entityTypeId  entity type id.
     * @param entityLocalId local entity id within entity type.
     * @param version       entity version.
     */
    public PersistentEntityId(final int entityTypeId, final long entityLocalId, final int version) {
        this.entityTypeId = entityTypeId;
        this.entityLocalId = entityLocalId;
    }

    /**
     * Generic constructor for id of the last (up-to-date) version of entity.
     *
     * @param entityTypeId  entity type id.
     * @param entityLocalId local entity id within entity type.
     */
    public PersistentEntityId(final int entityTypeId, final long entityLocalId) {
        if (entityLocalId < 0) {
            throw new ExodusException("LocalId can't be negative");
        }
        this.entityTypeId = entityTypeId;
        this.entityLocalId = entityLocalId;
    }

    /**
     * Constructs id for last (up-to-date) version of specified id.
     *
     * @param id source entity id.
     */
    public PersistentEntityId(@NotNull final EntityId id) {
        this(id.getTypeId(), id.getLocalId());
    }

    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PersistentEntityId)) {
            return false;
        }
        final PersistentEntityId that = (PersistentEntityId) obj;
        return entityLocalId == that.entityLocalId &&
                entityTypeId == that.entityTypeId;
    }

    public int hashCode() {
        return (int) (entityTypeId << 20 ^ entityLocalId);
    }

    @Override
    public int getTypeId() {
        return entityTypeId;
    }

    @Override
    public long getLocalId() {
        return entityLocalId;
    }

    @NotNull
    public String toString() {
        final StringBuilder builder = new StringBuilder(10);
        toString(builder);
        return builder.toString();
    }

    public void toString(@NotNull final StringBuilder builder) {
        builder.append(entityTypeId);
        builder.append('-');
        builder.append(entityLocalId);
    }

    public void toHash(@NotNull final EntityIterableHandleBase.EntityIterableHandleHash hash) {
        hash.apply(entityTypeId);
        hash.applyDelimiter();
        hash.apply(entityLocalId);
    }

    public static EntityId toEntityId(@NotNull final CharSequence representation) {
        return EntityIdCache.getEntityId(representation);
    }

    @Override
    public int compareTo(@NotNull final EntityId o) {
        final long rightLocalId = o.getLocalId();
        final int rightType = o.getTypeId();
        if (entityTypeId < rightType) {
            return -2;
        }
        if (entityTypeId > rightType) {
            return 2;
        }
        return Long.compare(entityLocalId, rightLocalId);
    }
}
