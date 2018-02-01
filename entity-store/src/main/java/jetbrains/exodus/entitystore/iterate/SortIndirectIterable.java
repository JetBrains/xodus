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

import jetbrains.exodus.entitystore.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"RawUseOfParameterizedType", "ConstructorWithTooManyParameters"})
public class SortIndirectIterable extends EntityIterableDecoratorBase {

    @NotNull
    private final String entityType;
    @NotNull
    private final EntityIterableBase sortedLinks;
    @NotNull
    private final String linkName;
    private final int sourceTypeId;
    private final int linkId;
    @Nullable
    private final String oppositeEntityType;
    @Nullable
    private final String oppositeLinkName;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                int typeId = Integer.parseInt((String) parameters[0]);
                String typeName = null;
                for (String type : store.getEntityTypes(txn)) {
                    if (typeId == store.getEntityTypeId(txn, type, false)) {
                        typeName = type;
                    }
                }
                int linkId = Integer.parseInt((String) parameters[1]);
                String linkName = null;
                for (String name : store.getAllLinkNames(txn)) {
                    if (linkId == store.getLinkId(txn, name, false)) {
                        linkName = name;
                    }
                }
                return new SortIndirectIterable(txn, store, typeName,
                        (EntityIterableBase) parameters[3], linkName, (EntityIterableBase) parameters[2],
                        null, null);
            }
        });
    }

    public SortIndirectIterable(@NotNull final PersistentStoreTransaction txn,
                                @NotNull final PersistentEntityStoreImpl store,
                                @NotNull final String entityType,
                                @NotNull final EntityIterableBase sortedLinks,
                                @NotNull final String linkName,
                                @NotNull final EntityIterableBase source,
                                @Nullable final String oppositeEntityType,
                                @Nullable final String oppositeLinkName) {
        super(txn, source);
        this.entityType = entityType;
        this.sortedLinks = sortedLinks;
        this.linkName = linkName;
        sourceTypeId = store.getEntityTypeId(txn, entityType, false);
        linkId = store.getLinkId(txn, linkName, false);
        this.oppositeEntityType = oppositeEntityType;
        this.oppositeLinkName = oppositeLinkName;
    }

    @Override
    public boolean setOrigin(Object origin) {
        if (super.setOrigin(origin)) {
            sortedLinks.setOrigin(origin);
            return true;
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        return source.isEmpty();
    }

    @Override
    public long size() {
        return source.size();
    }

    @Override
    public long count() {
        return source.count();
    }

    @Override
    public long getRoughCount() {
        return source.getRoughCount();
    }

    @Override
    public long getRoughSize() {
        return source.getRoughSize();
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        int count = 0;
        final EntityIterator sorted = source.iterator();
        while (sorted.hasNext()) {
            final EntityId entityId = sorted.nextId();
            if (entityId == null || sourceTypeId == entityId.getTypeId()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean canBeCached() {
        return super.canBeCached() && sortedLinks.canBeCached();
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, new SortIndirectIterator(txn));
    }

    private static EntityIterableType getType() {
        return EntityIterableType.SORTING_LINKS;
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), getType(), source.getHandle()) {
            private final EntityIterableHandleBase sortedLinksHandle = (EntityIterableHandleBase) sortedLinks.getHandle();

            @NotNull
            private final int[] linkIds = mergeFieldIds(new int[]{linkId}, mergeFieldIds(decorated.getLinkIds(), sortedLinksHandle.getLinkIds()));

            @NotNull
            @Override
            public int[] getLinkIds() {
                return linkIds;
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(sourceTypeId);
                builder.append('-');
                builder.append(linkId);
                builder.append('-');
                applyDecoratedToBuilder(builder);
                builder.append('-');
                sortedLinksHandle.toString(builder);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(sourceTypeId);
                hash.applyDelimiter();
                hash.apply(linkId);
                hash.applyDelimiter();
                super.hashCode(hash);
                hash.applyDelimiter();
                hash.apply(sortedLinksHandle);
            }

            @Override
            public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
                return decorated.isMatchedEntityAdded(added) ||
                        sortedLinksHandle.isMatchedEntityAdded(added);
            }

            @Override
            public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
                return decorated.isMatchedEntityDeleted(deleted) ||
                        sortedLinksHandle.isMatchedEntityDeleted(deleted);
            }

            @Override
            public boolean isMatchedLinkAdded(@NotNull final EntityId source,
                                              @NotNull final EntityId target,
                                              final int linkId) {
                if (SortIndirectIterable.this.linkId == linkId) {
                    return true;
                }
                if (decorated.hasLinkId(linkId)) {
                    if (decorated.isMatchedLinkAdded(source, target, linkId)) {
                        return true;
                    }
                    if (!sortedLinksHandle.hasLinkId(linkId)) {
                        return false;
                    }
                }
                return sortedLinksHandle.isMatchedLinkAdded(source, target, linkId);
            }

            @Override
            public boolean isMatchedLinkDeleted(@NotNull final EntityId source,
                                                @NotNull final EntityId target,
                                                final int linkId) {
                if (SortIndirectIterable.this.linkId == linkId) {
                    return true;
                }
                if (decorated.hasLinkId(linkId)) {
                    if (decorated.isMatchedLinkDeleted(source, target, linkId)) {
                        return true;
                    }
                    if (!sortedLinksHandle.hasLinkId(linkId)) {
                        return false;
                    }
                }
                return sortedLinksHandle.isMatchedLinkDeleted(source, target, linkId);
            }

            @Override
            public boolean isMatchedPropertyChanged(@NotNull EntityId id,
                                                    final int propertyId,
                                                    @Nullable final Comparable oldValue,
                                                    @Nullable final Comparable newValue) {
                return decorated.isMatchedPropertyChanged(id, propertyId, oldValue, newValue) ||
                        sortedLinksHandle.isMatchedPropertyChanged(id, propertyId, oldValue, newValue);
            }
        };
    }

    private final class SortIndirectIterator extends NonDisposableEntityIterator {

        private EntityIterator linksIterator;
        private EntityIterator foundLinksIterator;
        private EntityId nextId;
        private boolean nullIterated;
        private final PersistentStoreTransaction txn;

        SortIndirectIterator(final PersistentStoreTransaction txn) {
            super(SortIndirectIterable.this);
            linksIterator = null;
            foundLinksIterator = null;
            nextId = null;
            nullIterated = false;
            this.txn = txn;
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        protected boolean hasNextImpl() {
            checkInited();
            for (; ; ) {
                if (foundLinksIterator != null) {
                    if (!foundLinksIterator.hasNext()) {
                        foundLinksIterator = null;
                    } else {
                        nextId = foundLinksIterator.nextId();
                        return true;
                    }
                } else {
                    if (!linksIterator.hasNext()) {
                        if (nullIterated) {
                            nextId = null;
                            break;
                        }
                        nullIterated = true;
                        //noinspection ConstantConditions
                        foundLinksIterator = //txn.getAll(entityType).intersectSavingOrder(source).
                                new FilterEntityTypeIterable(txn, sourceTypeId, source).
                                        minus(oppositeEntityType == null ?
                                                txn.findWithLinks(entityType, linkName) :
                                                txn.findWithLinks(entityType, linkName, oppositeEntityType, oppositeLinkName)
                                        ).iterator();
                    } else {
                        final EntityId linkId = linksIterator.nextId();
                        final Entity link = linkId == null ? null : getEntity(linkId);
                        if (link == null) {
                            nullIterated = true;
                            //noinspection ConstantConditions
                            foundLinksIterator = //txn.getAll(entityType).intersectSavingOrder(source).
                                    new FilterEntityTypeIterable(txn, sourceTypeId, source).
                                            minus(oppositeEntityType == null ?
                                                    txn.findWithLinks(entityType, linkName) :
                                                    txn.findWithLinks(entityType, linkName, oppositeEntityType, oppositeLinkName)
                                            ).iterator();
                        } else {
                            foundLinksIterator = txn.findLinks(entityType, link, linkName).intersectSavingOrder(source).iterator();
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public EntityId nextIdImpl() {
            return nextId;
        }

        private void checkInited() {
            if (linksIterator == null) {
                linksIterator = sortedLinks.iterator();
            }
        }
    }
}
