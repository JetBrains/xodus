/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates all entities of specified type which are linked to specified entity with specified link.
 */
public final class EntityToLinksIterable extends EntityLinksIterableBase {

    private final int entityTypeId;
    private final int linkId;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new EntityToLinksIterable(txn,
                        new PersistentEntityId(Integer.valueOf((String) parameters[0]),
                                Integer.valueOf((String) parameters[1])),
                        Integer.valueOf((String) parameters[2]), Integer.valueOf((String) parameters[3])
                );
            }
        });
    }

    public EntityToLinksIterable(@NotNull final PersistentStoreTransaction txn,
                                 @NotNull final EntityId entityId,
                                 final int entityTypeId,
                                 final int linkId) {
        super(txn, entityId);
        this.entityTypeId = entityTypeId;
        this.linkId = linkId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ENTITY_TO_LINKS;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new LinksIterator(openCursor(txn));
    }

    @Override
    public boolean nonCachedHasFastCountAndIsEmpty() {
        return true;
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), EntityToLinksIterable.getType()) {

            @NotNull
            @Override
            public int[] getLinkIds() {
                return new int[]{linkId};
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                ((PersistentEntityId) entityId).toString(builder);
                builder.append('-');
                builder.append(entityTypeId);
                builder.append('-');
                builder.append(linkId);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                ((PersistentEntityId) entityId).toHash(hash);
                hash.applyDelimiter();
                hash.apply(entityTypeId);
                hash.applyDelimiter();
                hash.apply(linkId);
            }

            @Override
            public int getEntityTypeId() {
                return entityTypeId;
            }

            @Override
            public boolean isMatchedLinkAdded(@NotNull final EntityId source,
                                              @NotNull final EntityId target,
                                              final int linkId) {
                return entityTypeId == source.getTypeId() && entityId.equals(target);
            }

            @Override
            public boolean isMatchedLinkDeleted(@NotNull final EntityId source,
                                                @NotNull final EntityId target,
                                                final int linkId) {
                return isMatchedLinkAdded(source, target, linkId);
            }
        };
    }

    @Override
    public @Nullable Entity getLast() {
        final PersistentStoreTransaction txn = getStore().getAndCheckCurrentTransaction();
        try (Cursor cursor = openCursor(txn)) {
            if (cursor.getSearchKeyRange(getKey(new PersistentEntityId(entityId.getTypeId(), entityId.getLocalId() + 1))) == null) {
                if (!cursor.getLast()) {
                    return null;
                }
            } else {
                if (!cursor.getPrev()) {
                    return null;
                }
            }
            final LinkValue key = LinkValue.entryToLinkValue(cursor.getKey());
            if (!key.getEntityId().equals(entityId) || key.getLinkId() != linkId) {
                return null;
            }
            return txn.getEntity(new PersistentEntityId(entityTypeId, PropertyKey.entryToPropertyKey(cursor.getValue()).getEntityLocalId()));
        }
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return new SingleKeyCursorCounter(openCursor(txn), getFirstKey()).getCount();
    }

    @Override
    public boolean isEmptyImpl(@NotNull final PersistentStoreTransaction txn) {
        return new SingleKeyCursorIsEmptyChecker(openCursor(txn), getFirstKey()).isEmpty();
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getLinksSecondIndexCursor(txn, entityTypeId);
    }

    private ByteIterable getFirstKey() {
        return getKey(entityId);
    }

    private ByteIterable getKey(final EntityId entityId) {
        return LinkValue.linkValueToEntry(new LinkValue(entityId, linkId));
    }

    private final class LinksIterator extends EntityIteratorBase {

        private boolean hasNext;

        private LinksIterator(@NotNull final Cursor index) {
            super(EntityToLinksIterable.this);
            setCursor(index);
            hasNext = index.getSearchKey(getFirstKey()) != null;
        }

        @Override
        public boolean hasNextImpl() {
            return hasNext;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                explain(getType());
                final Cursor cursor = getCursor();
                final EntityId result = new PersistentEntityId(entityTypeId, PropertyKey.entryToPropertyKey(cursor.getValue()).getEntityLocalId());
                hasNext = cursor.getNextDup();
                return result;
            }
            return null;
        }
    }
}
