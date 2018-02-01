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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.entitystore.iterate.EntityAddedOrDeletedHandleChecker;
import jetbrains.exodus.entitystore.iterate.LinkChangedHandleChecker;
import jetbrains.exodus.entitystore.iterate.PropertyChangedHandleChecker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface EntityIterableHandle {

    @NotNull
    EntityIterableType getType();

    Object getIdentity();

    boolean isSticky();

    boolean isMatchedEntityAdded(@NotNull EntityId added);

    boolean isMatchedEntityDeleted(@NotNull EntityId deleted);

    boolean isMatchedLinkAdded(@NotNull EntityId source, @NotNull EntityId target, int linkId);

    boolean isMatchedLinkDeleted(@NotNull EntityId source, @NotNull EntityId target, int linkId);

    boolean isMatchedPropertyChanged(@NotNull EntityId id, int propertyId,
                                     @Nullable Comparable oldValue, @Nullable Comparable newValue);

    boolean onEntityAdded(@NotNull EntityAddedOrDeletedHandleChecker handleChecker);

    boolean onEntityDeleted(@NotNull EntityAddedOrDeletedHandleChecker handleChecker);

    boolean onLinkAdded(@NotNull LinkChangedHandleChecker handleChecker);

    boolean onLinkDeleted(@NotNull LinkChangedHandleChecker handleChecker);

    boolean onPropertyChanged(@NotNull PropertyChangedHandleChecker handleChecker);

    int getEntityTypeId();

    @NotNull
    int[] getLinkIds();

    @NotNull
    int[] getPropertyIds();

    @NotNull
    int[] getTypeIdsAffectingCreation();

    boolean hasLinkId(int id);

    boolean isConsistent();

    void resetBirthTime();

    boolean isExpired();
}
