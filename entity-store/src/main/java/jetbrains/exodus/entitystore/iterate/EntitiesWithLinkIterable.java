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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EntitiesWithLinkIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final int linkId;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new EntitiesWithLinkIterable(txn,
                        Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]));
            }
        });
    }

    public EntitiesWithLinkIterable(@NotNull final PersistentStoreTransaction txn,
                                    final int entityTypeId,
                                    final int linkId) {
        super(txn);
        this.entityTypeId = entityTypeId;
        this.linkId = linkId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_WITH_LINK;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    public boolean canBeReordered() {
        return true;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new LinksIterator(openCursor(txn));
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntitiesWithLinkIterableHandle();
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getLinksSecondIndexCursor(txn, entityTypeId);
    }

    private final class LinksIterator extends EntityIteratorBase {

        private boolean hasNext;

        private LinksIterator(@NotNull final Cursor index) {
            super(EntitiesWithLinkIterable.this);
            setCursor(index);
            final ByteIterable key = LinkValue.linkValueToEntry(new LinkValue(PersistentEntityId.EMPTY_ID, linkId));
            hasNext = index.getSearchKeyRange(key) != null;
            checkCursorKey();
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
                final PropertyKey key = PropertyKey.entryToPropertyKey(cursor.getValue());
                final EntityId result = new PersistentEntityId(entityTypeId, key.getEntityLocalId());
                hasNext = cursor.getNext();
                checkCursorKey();
                return result;
            }
            return null;
        }

        private void checkCursorKey() {
            if (hasNext) {
                final LinkValue value = LinkValue.entryToLinkValue(getCursor().getKey());
                hasNext = value.getLinkId() == linkId;
            }
        }
    }

    protected class EntitiesWithLinkIterableHandle extends ConstantEntityIterableHandle {

        public EntitiesWithLinkIterableHandle() {
            super(EntitiesWithLinkIterable.this.getStore(), EntitiesWithLinkIterable.getType());
        }

        @NotNull
        @Override
        public int[] getLinkIds() {
            return new int[]{linkId};
        }

        @Override
        public void toString(@NotNull final StringBuilder builder) {
            super.toString(builder);
            builder.append(entityTypeId);
            builder.append('-');
            builder.append(linkId);
        }

        @Override
        public void hashCode(@NotNull final EntityIterableHandleHash hash) {
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
            return entityTypeId == source.getTypeId();
        }

        @Override
        public boolean isMatchedLinkDeleted(@NotNull final EntityId source,
                                            @NotNull final EntityId target,
                                            final int linkId) {
            return entityTypeId == source.getTypeId();
        }
    }
}
