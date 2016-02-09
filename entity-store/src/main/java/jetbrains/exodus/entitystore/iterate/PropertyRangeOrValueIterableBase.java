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

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

abstract class PropertyRangeOrValueIterableBase extends EntityIterableBase {

    private final int entityTypeId;
    private final int propertyId;

    public PropertyRangeOrValueIterableBase(@Nullable final PersistentEntityStoreImpl store,
                                            final int entityTypeId,
                                            final int propertyId) {
        super(store);
        this.entityTypeId = entityTypeId;
        this.propertyId = propertyId;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    public int getPropertyId() {
        return propertyId;
    }

    @Override
    public boolean canBeCached() {
        return !getPropertyValueIndex().isCachedInstance();
    }

    protected Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getPropertyValuesIndexCursor(txn, entityTypeId, propertyId);
    }

    protected EntityIterableBase getPropertyValueIndex() {
        return getStore().getEntityIterableCache().putIfNotCached(new PropertiesIterable(getStore(), entityTypeId, propertyId));
    }
}
