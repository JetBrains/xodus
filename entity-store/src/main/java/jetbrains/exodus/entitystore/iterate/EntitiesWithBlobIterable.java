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

import jetbrains.exodus.entitystore.*;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.entitystore.DualCompatibilityKt.asPersistent;

public class EntitiesWithBlobIterable extends EntityIterableBase {

    private final int entityTypeId;
    private final int blobId;

    static {
        registerType(getType(), (txn, store, parameters) -> new EntitiesWithBlobIterable(txn,
            Integer.parseInt((String) parameters[0]), Integer.parseInt((String) parameters[1])));
    }

    public EntitiesWithBlobIterable(@NotNull final StoreTransaction txn, final int entityTypeId, final int blobId) {
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
    public EntityIterator getIteratorImpl(@NotNull final StoreTransaction txn) {
        return new FieldIndexIterator(this, entityTypeId, blobId,
            getStoreImpl().getEntityWithBlobIterable(asPersistent(txn), entityTypeId, blobId));
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
}
