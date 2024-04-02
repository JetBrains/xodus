/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates all entities which specified entity is linked with specified link id.
 */
public class EntityFromLinksIterable extends EntityLinksIterableBase {

    private final int linkId;

    static {
        registerType(getType(), (txn, store, parameters) -> new EntityFromLinksIterable(txn,
                new PersistentEntityId(Integer.parseInt((String) parameters[0]),
                        Integer.parseInt((String) parameters[1])), Integer.parseInt((String) parameters[2])
        ));
    }

    public EntityFromLinksIterable(@NotNull final StoreTransaction txn,
                                   @NotNull final EntityId entityId,
                                   final int linkId) {
        super(txn, entityId);
        this.linkId = linkId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ENTITY_FROM_LINKS;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final StoreTransaction txn) {
        return new LinksIterator(openCursor(txn));
    }

    @Override
    public boolean nonCachedHasFastCountAndIsEmpty() {
        return true;
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), EntityFromLinksIterable.getType()) {

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
                builder.append(linkId);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                ((PersistentEntityId) entityId).toHash(hash);
                hash.applyDelimiter();
                hash.apply(linkId);
            }

            @Override
            public boolean isMatchedLinkAdded(@NotNull final EntityId source,
                                              @NotNull final EntityId target,
                                              final int linkId) {
                return entityId.equals(source);
            }

            @Override
            public boolean isMatchedLinkDeleted(@NotNull final EntityId source,
                                                @NotNull final EntityId target,
                                                final int linkId) {
                return entityId.equals(source);
            }
        };
    }

    @Override
    @Nullable
    public Entity getLast() {
        final StoreTransaction txn = getStore().getAndCheckCurrentTransaction();
        try (Cursor cursor = openCursor(txn)) {
            if (cursor.getSearchKeyRange(getKey(linkId + 1)) == null) {
                if (!cursor.getLast()) {
                    return null;
                }
            } else {
                if (!cursor.getPrev()) {
                    return null;
                }
            }
            final PropertyKey key = PropertyKey.entryToPropertyKey(cursor.getKey());
            if (key.getEntityLocalId() != entityId.getLocalId() || key.getPropertyId() != linkId) {
                return null;
            }
            final LinkValue value = LinkValue.entryToLinkValue(cursor.getValue());
            return txn.getEntity(value.getEntityId());
        }
    }

    @Override
    protected long countImpl(@NotNull final StoreTransaction txn) {
        return new SingleKeyCursorCounter(openCursor(txn), getFirstKey()).getCount();
    }

    @Override
    public boolean isEmptyImpl(@NotNull final StoreTransaction txn) {
        return new SingleKeyCursorIsEmptyChecker(openCursor(txn), getFirstKey()).isEmpty();
    }

    private Cursor openCursor(@NotNull final StoreTransaction txn) {
        return ((PersistentEntityStoreImpl) getStore()).getLinksFirstIndexCursor((PersistentStoreTransaction) txn, entityId.getTypeId());
    }

    private ByteIterable getFirstKey() {
        return getKey(linkId);
    }

    private ByteIterable getKey(final int linkId) {
        return PropertyKey.propertyKeyToEntry(new PropertyKey(entityId.getLocalId(), linkId));
    }

    private final class LinksIterator extends EntityIteratorBase {

        private boolean hasNext;

        private LinksIterator(@NotNull final Cursor index) {
            super(EntityFromLinksIterable.this);
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
                final LinkValue value = LinkValue.entryToLinkValue(getCursor().getValue());
                final EntityId result = value.getEntityId();
                hasNext = getCursor().getNextDup();
                return result;
            }
            return null;
        }
    }
}