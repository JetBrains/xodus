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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyHistoryKey;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class EntityFromHistoryLinksIterable extends EntityLinksIterableBase {

    private final int version;
    private final int linkId;

    public EntityFromHistoryLinksIterable(@NotNull final PersistentEntityStoreImpl store,
                                          @NotNull final EntityId entityId,
                                          final int version,
                                          final int linkId) {
        super(store, entityId);
        this.version = version;
        this.linkId = linkId;
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new LinksIterator(openCursor(txn), getFirstKey());
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), EntityIterableType.HISTORY_ENTITY_FROM_LINKS) {

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
        };
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return new SingleKeyCursorCounter(openCursor(txn), getFirstKey()).getCount();
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getLinksHistoryIndexCursor(txn, entityId.getTypeId());
    }

    private ByteIterable getFirstKey() {
        return PropertyHistoryKey.propertyHistoryKeyToEntry(new PropertyHistoryKey(entityId.getLocalId(), version, linkId));
    }

    private final class LinksIterator extends EntityIteratorBase {

        private boolean hasNext;

        private LinksIterator(@NotNull final Cursor index, @NotNull final ByteIterable key) {
            super(EntityFromHistoryLinksIterable.this);
            setCursor(index);
            hasNext = index.getSearchKey(key) != null;
        }

        @Override
        public boolean hasNextImpl() {
            return hasNext;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                final LinkValue value = LinkValue.entryToLinkValue(getCursor().getValue());
                final EntityId result = value.getEntityId();
                hasNext = getCursor().getNextDup();
                return result;
            }
            return null;
        }
    }
}
