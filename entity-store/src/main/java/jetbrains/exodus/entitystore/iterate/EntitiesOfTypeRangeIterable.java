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
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates all entities of specified entity type in range of local ids.
 */
public class EntitiesOfTypeRangeIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final long min;
    private final long max;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                long min = Long.parseLong((String) parameters[1]);
                long max = Long.parseLong((String) parameters[2]);
                return new EntitiesOfTypeRangeIterable(txn, Integer.valueOf((String) parameters[0]), min, max);
            }
        });
    }

    public EntitiesOfTypeRangeIterable(@NotNull final PersistentStoreTransaction txn,
                                       final int entityTypeId, final long min, final long max) {
        super(txn);
        this.entityTypeId = entityTypeId;
        this.min = min;
        this.max = max;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ALL_ENTITIES_RANGE;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntitiesOfTypeIterator(this, openCursor(txn));
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getEntitiesIndexCursor(txn, entityTypeId);
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), EntitiesOfTypeRangeIterable.getType()) {

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(entityTypeId);
                builder.append('-');
                builder.append(min);
                builder.append('-');
                builder.append(max);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(entityTypeId);
                hash.applyDelimiter();
                hash.apply(min);
                hash.applyDelimiter();
                hash.apply(max);
            }

            @NotNull
            @Override
            public int[] getTypeIdsAffectingCreation() {
                // TODO: if open ranges are prohibited, we can improve this
                return new int[]{entityTypeId};
            }

            @Override
            public int getEntityTypeId() {
                return entityTypeId;
            }

            @Override
            public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
                return added.getTypeId() == entityTypeId && isRangeAffected(added.getLocalId());
            }

            @Override
            public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
                return deleted.getTypeId() == entityTypeId && isRangeAffected(deleted.getLocalId());
            }

            private boolean isRangeAffected(long id) {
                return min <= id && id <= max;
            }
        };
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        final Cursor cursor = openCursor(txn);
        if (cursor == null) {
            return 0;
        }
        try {
            final ByteIterable key = LongBinding.longToCompressedEntry(min);
            long result = 0;
            boolean success = cursor.getSearchKeyRange(key) != null;
            while (success) {
                if (max > LongBinding.compressedEntryToLong(cursor.getKey())) {
                    break;
                }
                result++;
                success = cursor.getNextNoDup();
            }
            return result;
        } finally {
            cursor.close();
        }
    }

    private final class EntitiesOfTypeIterator extends EntityIteratorBase {

        private boolean hasNext;

        private EntitiesOfTypeIterator(@NotNull final EntitiesOfTypeRangeIterable iterable,
                                       @NotNull final Cursor index) {
            super(iterable);
            setCursor(index);
            final ByteIterable key = LongBinding.longToCompressedEntry(min);
            checkHasNext(getCursor().getSearchKeyRange(key) != null);
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
                final long localId = LongBinding.compressedEntryToLong(cursor.getKey());
                final EntityId result = new PersistentEntityId(entityTypeId, localId);
                checkHasNext(cursor.getNext());
                return result;
            }
            return null;
        }

        private void checkHasNext(final boolean success) {
            hasNext = success && max >= LongBinding.compressedEntryToLong(getCursor().getKey());
        }
    }

}
