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
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.iterate.cached.SingleTypeUnsortedEntityIdArrayCachedInstanceIterable;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;

/**
 * Iterates over entities of specified entity type having specified link to a target.
 */
class EntitiesWithCertainLinkIterable extends EntityIterableBase {

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new EntitiesWithCertainLinkIterable(txn,
                    Integer.valueOf((String) parameters[0]),
                    Integer.valueOf((String) parameters[1]));
            }
        });
    }

    private final int entityTypeId;
    private final int linkId;

    EntitiesWithCertainLinkIterable(@NotNull final PersistentStoreTransaction txn,
                                    final int entityTypeId,
                                    final int linkId) {
        super(txn);
        this.entityTypeId = entityTypeId;
        this.linkId = linkId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_WITH_CERTAIN_LINK;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    @NotNull
    public LinksIteratorWithTarget getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new LinksIterator(openCursor(txn));
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), getType()) {

            @NotNull
            @Override
            public int[] getLinkIds() {
                return new int[]{linkId};
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append('-');
                builder.append(entityTypeId);
                builder.append('-');
                builder.append(linkId);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
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
                return entityTypeId == source.getTypeId();
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
    public boolean isSortedById() {
        return false;
    }

    @Override
    protected CachedInstanceIterable createCachedInstance(@NotNull final PersistentStoreTransaction txn) {
        final LongArrayList localIds = new LongArrayList();
        final ArrayList<EntityId> targets = new ArrayList<>();
        final LinksIteratorWithTarget it = getIteratorImpl(txn);
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        while (it.hasNext()) {
            @SuppressWarnings("ConstantConditions") final long localId = it.nextId().getLocalId();
            localIds.add(localId);
            targets.add(it.getTarget());
            if (min > localId) {
                min = localId;
            }
            if (max < localId) {
                max = localId;
            }
        }
        return new CachedLinksIterable(txn, localIds.toArray(), targets.toArray(new EntityId[0]), min, max);
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getLinksSecondIndexCursor(txn, entityTypeId);
    }

    abstract class LinksIteratorWithTarget extends EntityIteratorBase {

        LinksIteratorWithTarget(@NotNull final EntityIterableBase iterable) {
            super(iterable);
        }

        abstract EntityId getTarget();
    }

    private final class LinksIterator extends LinksIteratorWithTarget {

        private PropertyKey key;
        private EntityId targetId;

        private LinksIterator(@NotNull final Cursor index) {
            super(EntitiesWithCertainLinkIterable.this);
            setCursor(index);
            final ByteIterable key = LinkValue.linkValueToEntry(new LinkValue(new PersistentEntityId(0, 0L), linkId));
            if (index.getSearchKeyRange(key) != null) {
                loadCursorState();
            }
        }

        @Override
        public boolean hasNextImpl() {
            if (key == null) {
                if (getCursor().getNext()) {
                    loadCursorState();
                }
                return key != null;
            }
            return true;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                final EntityId result = new PersistentEntityId(entityTypeId, key.getEntityLocalId());
                key = null;
                return result;
            }
            return null;
        }

        @Override
        public EntityId getTarget() {
            return targetId;
        }

        private void loadCursorState() {
            final Cursor cursor = getCursor();
            final LinkValue link = LinkValue.entryToLinkValue(cursor.getKey());
            if (link.getLinkId() == linkId) {
                this.key = PropertyKey.entryToPropertyKey(cursor.getValue());
                targetId = link.getEntityId();
            }
        }
    }

    private final class CachedLinksIterable extends SingleTypeUnsortedEntityIdArrayCachedInstanceIterable {

        private final long[] localIds;
        private final EntityId[] targets;

        CachedLinksIterable(@NotNull final PersistentStoreTransaction txn,
                            @NotNull final long[] localIds,
                            @NotNull final EntityId[] targets,
                            long min, long max) {
            super(txn, EntitiesWithCertainLinkIterable.this, entityTypeId, localIds, null, min, max);
            this.localIds = localIds;
            this.targets = targets;
        }

        @Override
        @NotNull
        public LinksIteratorWithTarget getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
            return new LinksIteratorWithTarget(CachedLinksIterable.this) {

                private int i = 0;
                private EntityId target;

                @Override
                EntityId getTarget() {
                    return target;
                }

                @Override
                protected boolean hasNextImpl() {
                    return i < localIds.length;
                }

                @Override
                protected EntityId nextIdImpl() {
                    target = targets[i];
                    return new PersistentEntityId(entityTypeId, localIds[i++]);
                }
            };
        }
    }
}
