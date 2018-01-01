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
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Set;

@SuppressWarnings({"RawUseOfParameterizedType"})
public class SelectManyDistinctIterable extends EntityIterableDecoratorBase {

    private final int linkId;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new SelectManyDistinctIterable(txn,
                        (EntityIterableBase) parameters[1], Integer.valueOf((String) parameters[0]));
            }
        });
    }

    public SelectManyDistinctIterable(@NotNull final PersistentStoreTransaction txn,
                                      @NotNull final EntityIterableBase source,
                                      final int linkId) {
        super(txn, source);
        this.linkId = linkId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.SELECTMANY_DISTINCT;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, new SelectManyDistinctIterator(txn));
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), SelectManyDistinctIterable.getType(), source.getHandle()) {

            @NotNull
            private final int[] linkIds = mergeFieldIds(new int[]{linkId}, decorated.getLinkIds());

            @NotNull
            @Override
            public int[] getLinkIds() {
                return linkIds;
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                applyDecoratedToBuilder(builder);
                builder.append('-');
                builder.append(linkId);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                super.hashCode(hash);
                hash.applyDelimiter();
                hash.apply(linkId);
            }

            @Override
            public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
                return decorated.isMatchedEntityAdded(added);
            }

            @Override
            public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
                return decorated.isMatchedEntityDeleted(deleted);
            }

            @Override
            public boolean isMatchedLinkAdded(@NotNull final EntityId source, @NotNull final EntityId target, final int linkId) {
                return linkId == SelectManyDistinctIterable.this.linkId || decorated.isMatchedLinkAdded(source, target, linkId);
            }

            @Override
            public boolean isMatchedLinkDeleted(@NotNull final EntityId source, @NotNull final EntityId target, final int linkId) {
                return linkId == SelectManyDistinctIterable.this.linkId || decorated.isMatchedLinkDeleted(source, target, linkId);
            }

            @Override
            public boolean isMatchedPropertyChanged(final int typeId,
                                                    final int propertyId,
                                                    @Nullable final Comparable oldValue,
                                                    @Nullable final Comparable newValue) {
                return decorated.isMatchedPropertyChanged(typeId, propertyId, oldValue, newValue);
            }
        };
    }

    private class SelectManyDistinctIterator extends EntityIteratorBase {

        @NotNull
        private final EntityIteratorBase sourceIt;
        @NotNull
        private final IntHashMap<Cursor> usedCursors;
        private Set<EntityId> usedIds;
        @NotNull
        private final PersistentStoreTransaction txn;

        private SelectManyDistinctIterator(@NotNull final PersistentStoreTransaction txn) {
            super(SelectManyDistinctIterable.this);
            sourceIt = (EntityIteratorBase) source.iterator();
            usedCursors = new IntHashMap<>();
            usedIds = null;
            this.txn = txn;
        }

        @Override
        protected boolean hasNextImpl() {
            if (usedIds == null) {
                collectIds();
            }
            return !usedIds.isEmpty();
        }

        @SuppressWarnings({"ObjectAllocationInLoop"})
        private void collectIds() {
            final Set<EntityId> usedIds = new LinkedHashSet<>();
            this.usedIds = usedIds;
            final EntityIterator sourceIt = this.sourceIt;
            final int linkId = SelectManyDistinctIterable.this.linkId;
            if (linkId >= 0) {
                while (sourceIt.hasNext()) {
                    EntityId nextId = sourceIt.nextId();
                    if (nextId == null) {
                        continue;
                    }
                    final int typeId = nextId.getTypeId();
                    Cursor cursor = usedCursors.get(typeId);
                    if (cursor == null) {
                        cursor = getStore().getLinksFirstIndexCursor(txn, typeId);
                        usedCursors.put(typeId, cursor);
                    }
                    final long sourceLocalId = nextId.getLocalId();
                    ByteIterable value = cursor.getSearchKey(PropertyKey.propertyKeyToEntry(new PropertyKey(sourceLocalId, linkId)));
                    if (value == null) {
                        usedIds.add(null);
                    } else {
                        for (; ; ) {
                            // value is updated automatically through every iteration
                            final LinkValue linkValue = LinkValue.entryToLinkValue(value);
                            nextId = linkValue.getEntityId();
                            usedIds.add(nextId);
                            if (!cursor.getNext()) {
                                break;
                            }
                            final PropertyKey key = PropertyKey.entryToPropertyKey(cursor.getKey());
                            if (key.getPropertyId() != linkId || key.getEntityLocalId() != sourceLocalId) {
                                break;
                            }
                            value = cursor.getValue(); // must be called for XD, because it returns different value pointer
                        }
                    }
                }
            }
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final Iterator<EntityId> it = usedIds.iterator();
            if (it.hasNext()) {
                final EntityId id = it.next();
                usedIds.remove(id);
                return id;
            }
            return null;
        }

        @Override
        public boolean dispose() {
            sourceIt.disposeIfShouldBe();
            return super.dispose() && usedCursors.forEachValue(new ObjectProcedure<Cursor>() {
                @Override
                public boolean execute(final Cursor object) {
                    object.close();
                    return true;
                }
            });
        }
    }
}
