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

import jetbrains.exodus.core.dataStructures.IntArrayList;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.EntityId;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;

class EntityIdArrayWithSetIteratorWrapper extends EntityFromLinkSetIteratorBase {

    @NonNls
    private final EntityIteratorBase wrappedIterator;
    private final IntArrayList propIds;
    private final IntHashMap<String> linkNames;

    @SuppressWarnings({"AssignmentToCollectionOrArrayFieldFromParameter"})
    EntityIdArrayWithSetIteratorWrapper(@NotNull EntityIterableBase iterable,
                                        @NotNull final EntityIteratorBase wrappedIterator,
                                        @NotNull IntArrayList propIds,
                                        @NotNull IntHashMap<String> linkNames) {
        super(iterable);
        this.wrappedIterator = wrappedIterator;
        this.propIds = propIds;
        this.linkNames = linkNames;
    }

    @Override
    public boolean skip(int number) {
        return wrappedIterator.skip(number);
    }

    @Override
    public EntityId nextId() {
        return wrappedIterator.nextId();
    }

    @Override
    public EntityId getLast() {
        return wrappedIterator.getLast();
    }

    @Override
    protected EntityId nextIdImpl() {
        return wrappedIterator.nextIdImpl();
    }

    @Override
    protected boolean hasNextImpl() {
        return wrappedIterator.hasNextImpl();
    }

    @Override
    public int currentPropId() {
        return propIds.get(wrappedIterator.getIndex() - 1);
    }

    @Override
    protected String getLinkName(final int linkId) {
        return linkNames.get(linkId);
    }
}
