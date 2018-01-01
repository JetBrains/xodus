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
import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.EntityIterableType;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"RawUseOfParameterizedType"})
public class EntityIterableHandleDecorator extends EntityIterableHandleBase {

    @NotNull
    protected final EntityIterableHandleBase decorated;

    protected EntityIterableHandleDecorator(@Nullable final PersistentEntityStore store,
                                            @NotNull final EntityIterableType type,
                                            @NotNull final EntityIterableHandle decorated) {
        super(store, type);
        this.decorated = (EntityIterableHandleBase) decorated;
    }

    @NotNull
    @Override
    public int[] getLinkIds() {
        return decorated.getLinkIds();
    }

    @Override
    @NotNull
    public int[] getPropertyIds() {
        return decorated.getPropertyIds();
    }

    @Override
    @NotNull
    public int[] getTypeIdsAffectingCreation() {
        return decorated.getTypeIdsAffectingCreation();
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
    public boolean isMatchedLinkAdded(@NotNull final EntityId source,
                                      @NotNull final EntityId target,
                                      final int linkId) {
        return decorated.isMatchedLinkAdded(source, target, linkId);
    }

    @Override
    public boolean isMatchedLinkDeleted(@NotNull final EntityId source,
                                        @NotNull final EntityId target,
                                        final int linkId) {
        return decorated.isMatchedLinkDeleted(source, target, linkId);
    }

    @Override
    public boolean isMatchedPropertyChanged(final int typeId,
                                            int propertyId,
                                            @Nullable final Comparable oldValue,
                                            @Nullable final Comparable newValue) {
        return decorated.isMatchedPropertyChanged(typeId, propertyId, oldValue, newValue);
    }

    @Override
    public boolean isExpired() {
        return decorated.isExpired();
    }

    @Override
    public void hashCode(@NotNull final EntityIterableHandleHash hash) {
        hash.apply(decorated);
    }

    protected void applyDecoratedToBuilder(@NotNull final StringBuilder builder) {
        decorated.toString(builder);
    }
}
