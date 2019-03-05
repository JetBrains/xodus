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
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;

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
        return new SelectManyDistinctIterator(txn);
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
        };
    }

    private class SelectManyDistinctIterator extends EntityIteratorBase implements SourceMappingIterator {

        @NotNull
        private final EntityIteratorBase sourceIt;
        @NotNull
        private final IntHashMap<Cursor> usedCursors;
        @NotNull
        private final Deque<Pair<EntityId, EntityId>> ids;
        @NotNull
        private final LightOutputStream auxStream;
        @NotNull
        private final int[] auxArray;
        private boolean idsCollected;
        private EntityId sourceId;
        @NotNull
        private final PersistentStoreTransaction txn;

        private SelectManyDistinctIterator(@NotNull final PersistentStoreTransaction txn) {
            super(SelectManyDistinctIterable.this);
            sourceIt = (EntityIteratorBase) source.iterator();
            usedCursors = new IntHashMap<>(6, 2.f);
            ids = new ArrayDeque<>();
            auxStream = new LightOutputStream();
            auxArray = new int[8];
            this.txn = txn;
        }

        @Override
        protected boolean hasNextImpl() {
            if (!idsCollected) {
                idsCollected = true;
                collectIds();
            }
            return !ids.isEmpty();
        }

        @SuppressWarnings("ConstantConditions")
        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (hasNextImpl()) {
                final Pair<EntityId, EntityId> pair = ids.poll();
                sourceId = pair.getFirst();
                return pair.getSecond();
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

        @Override
        @NotNull
        public EntityId getSourceId() {
            return sourceId;
        }

        @SuppressWarnings({"ObjectAllocationInLoop"})
        private void collectIds() {
            final EntityIterator sourceIt = this.sourceIt;
            final int linkId = SelectManyDistinctIterable.this.linkId;
            if (linkId >= 0) {
                EntityIdSet usedIds = EntityIdSetFactory.newSet();
                while (sourceIt.hasNext()) {
                    final EntityId sourceId = sourceIt.nextId();
                    if (sourceId == null) {
                        continue;
                    }
                    final int typeId = sourceId.getTypeId();
                    Cursor cursor = usedCursors.get(typeId);
                    if (cursor == null) {
                        cursor = getStore().getLinksFirstIndexCursor(txn, typeId);
                        usedCursors.put(typeId, cursor);
                    }
                    final long sourceLocalId = sourceId.getLocalId();
                    ByteIterable value = cursor.getSearchKey(
                        PropertyKey.propertyKeyToEntry(auxStream, auxArray, sourceLocalId, linkId));
                    if (value == null) {
                        if (!usedIds.contains(null)) {
                            usedIds = usedIds.add(null);
                            ids.add(new Pair<EntityId, EntityId>(sourceId, null));
                        }
                    } else {
                        for (; ; ) {
                            // value is updated automatically through every iteration
                            final LinkValue linkValue = LinkValue.entryToLinkValue(value);
                            final EntityId nextId = linkValue.getEntityId();
                            if (!usedIds.contains(nextId)) {
                                usedIds = usedIds.add(nextId);
                                ids.add(new Pair<>(sourceId, nextId));
                            }
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
    }
}
