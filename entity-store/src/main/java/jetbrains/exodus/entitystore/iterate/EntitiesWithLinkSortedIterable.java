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
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EntitiesWithLinkSortedIterable extends EntitiesWithLinkIterable {

    protected final int oppositeEntityTypeId;
    protected final int oppositeLinkId;

    public EntitiesWithLinkSortedIterable(@NotNull final PersistentStoreTransaction txn,
                                          final int entityTypeId,
                                          final int linkId,
                                          final int oppositeEntityTypeId,
                                          final int oppositeLinkId) {
        super(txn, entityTypeId, linkId);
        this.oppositeEntityTypeId = oppositeEntityTypeId;
        this.oppositeLinkId = oppositeLinkId;
    }

    @Override
    public boolean isSortedById() {
        return true;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new LinksIterator(openCursor(txn));
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntitiesWithLinkIterableHandle() {
            @Override
            public void toString(@NotNull StringBuilder builder) {
                super.toString(builder);
                builder.append('-');
                builder.append(oppositeEntityTypeId);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                super.hashCode(hash);
                hash.applyDelimiter();
                hash.apply(oppositeEntityTypeId);
            }
        };
    }

    @SuppressWarnings({"MethodOverridesPrivateMethodOfSuperclass"})
    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getLinksSecondIndexCursor(txn, oppositeEntityTypeId);
    }

    private final class LinksIterator extends EntityIteratorBase {

        private boolean hasNext;
        private LinkValue linkValue;

        private LinksIterator(@NotNull final Cursor index) {
            super(EntitiesWithLinkSortedIterable.this);
            setCursor(index);
            final ByteIterable key = LinkValue.linkValueToEntry(new LinkValue(new PersistentEntityId(getEntityTypeId(), 0), oppositeLinkId));
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
                final EntityId result = linkValue.getEntityId();
                hasNext = getCursor().getNext();
                checkCursorKey();
                return result;
            }
            return null;
        }

        private void checkCursorKey() {
            if (hasNext) {
                linkValue = LinkValue.entryToLinkValue(getCursor().getKey());
                hasNext = linkValue.getLinkId() == oppositeLinkId && linkValue.getEntityId().getTypeId() == getEntityTypeId();
            }
        }
    }
}
