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

import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.entitystore.EntityId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface EntityIdSet extends EntityIdCollection {

    EntityIdSet add(@Nullable final EntityId id);

    EntityIdSet add(final int typeId, final long localId);

    boolean contains(@Nullable final EntityId id);

    boolean contains(final int typeId, final long localId);

    boolean remove(@Nullable final EntityId id);

    boolean remove(final int typeId, final long localId);

    @NotNull
    LongSet getTypeSetSnapshot(int typeId);
}
