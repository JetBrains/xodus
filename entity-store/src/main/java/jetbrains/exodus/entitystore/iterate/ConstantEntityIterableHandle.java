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

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterableType;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Constant entity iterable handle corresponds to iterables not affected by transaction changes.
 */
@SuppressWarnings({"RawUseOfParameterizedType"})
public abstract class ConstantEntityIterableHandle extends EntityIterableHandleBase {

    protected ConstantEntityIterableHandle(@Nullable final PersistentEntityStore store,
                                           @NotNull final EntityIterableType type) {
        super(store, type);
    }

    // constant entity iterable handle never matches any changes
    @Override
    public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
        return false;
    }

    @Override
    public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
        return false;
    }

    @Override
    public boolean isMatchedLinkAdded(@NotNull final EntityId source, final @NotNull EntityId target, final int linkId) {
        return false;
    }

    @Override
    public boolean isMatchedLinkDeleted(@NotNull final EntityId source, @NotNull final EntityId target, final int linkId) {
        return false;
    }

    @Override
    public boolean isMatchedPropertyChanged(final int typeId,
                                            final int propertyId,
                                            @Nullable final Comparable oldValue,
                                            @Nullable final Comparable newValue) {
        return false;
    }
}
