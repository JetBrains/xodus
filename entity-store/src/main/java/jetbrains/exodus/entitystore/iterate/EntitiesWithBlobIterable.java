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

import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;

public class EntitiesWithBlobIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final int blobId;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new EntitiesWithBlobIterable(txn,
                        Integer.valueOf((String) parameters[0]), Integer.valueOf((String) parameters[1]));
            }
        });
    }

    public EntitiesWithBlobIterable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId, final int blobId) {
        super(txn);
        this.entityTypeId = entityTypeId;
        this.blobId = blobId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ENTITIES_WITH_BLOB;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new BlobsIterator(getStore().getEntityWithBlobCursor(txn, entityTypeId));
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntitiesWithBlobIterableHandle();
    }

    private final class EntitiesWithBlobIterableHandle extends ConstantEntityIterableHandle {

        private EntitiesWithBlobIterableHandle() {
            super(EntitiesWithBlobIterable.this.getStore(), EntitiesWithBlobIterable.getType());
        }

        @Override
        public void toString(@NotNull final StringBuilder builder) {
            super.toString(builder);
            builder.append(entityTypeId);
            builder.append('-');
            builder.append(blobId);
        }

        @Override
        public void hashCode(@NotNull final EntityIterableHandleHash hash) {
            hash.apply(entityTypeId);
            hash.applyDelimiter();
            hash.apply(blobId);
        }

        @Override
        public int getEntityTypeId() {
            return entityTypeId;
        }
    }

    public final class BlobsIterator extends EntityIteratorBase {

        private boolean hasNext;

        public BlobsIterator(@NotNull final Cursor cursor) {
            super(EntitiesWithBlobIterable.this);
            setCursor(cursor);
            hasNext = cursor.getSearchKey(IntegerBinding.intToCompressedEntry(blobId)) != null;
        }

        @Override
        protected boolean hasNextImpl() {
            return hasNext;
        }

        @Override
        protected EntityId nextIdImpl() {
            if (hasNext) {
                explain(getType());
                final Cursor cursor = getCursor();
                final long localId = LongBinding.compressedEntryToLong(cursor.getValue());
                final EntityId result = new PersistentEntityId(entityTypeId, localId);
                hasNext = cursor.getNextDup();
                return result;
            }
            return null;
        }
    }
}
