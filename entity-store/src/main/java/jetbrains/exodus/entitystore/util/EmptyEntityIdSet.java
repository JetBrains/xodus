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
package jetbrains.exodus.entitystore.util;

import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.iterate.EntityIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

class EmptyEntityIdSet implements EntityIdSet {
    private static final Set<EntityId> NOTHING = Collections.emptySet();

    EmptyEntityIdSet() {
    }

    @Override
    public EntityIdSet add(@Nullable EntityId id) {
        return new SingleTypeEntityIdSet(id);
    }

    @Override
    public EntityIdSet add(int typeId, long localId) {
        return new SingleTypeEntityIdSet(typeId, localId);
    }

    @Override
    public boolean contains(@Nullable EntityId id) {
        return false;
    }

    @Override
    public boolean contains(int typeId, long localId) {
        return false;
    }

    @Override
    public boolean remove(@Nullable EntityId id) {
        return false;
    }

    @Override
    public boolean remove(int typeId, long localId) {
        return false;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    @NotNull
    public LongSet getTypeSetSnapshot(int typeId) {
        return LongSet.EMPTY;
    }

    @Override
    public Iterator<EntityId> iterator() {
        return NOTHING.iterator();
    }
}
