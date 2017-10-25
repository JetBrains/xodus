/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.entitystore.*;
import org.jetbrains.annotations.NotNull;

public class SortResultIterable extends EntityIterableDecoratorBase {

    protected SortResultIterable(@NotNull final PersistentStoreTransaction txn,
                                 @NotNull final EntityIterableBase source) {
        super(txn, source);
    }

    @Override
    public boolean isEmpty() {
        return source.isEmpty();
    }

    @Override
    public long size() {
        return source.size();
    }

    @Override
    public long count() {
        return source.count();
    }

    @Override
    public long getRoughCount() {
        return source.getRoughCount();
    }

    @Override
    public long getRoughSize() {
        return source.getRoughSize();
    }

    @Override
    public boolean contains(@NotNull Entity entity) {
        return source.contains(entity);
    }

    @Override
    public int indexOf(@NotNull final Entity entity) {
        return indexOfImpl(entity.getId());
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    @Override
    public Entity getFirst() {
        return source.getFirst();
    }

    @Override
    public Entity getLast() {
        return source.getLast();
    }

    @NotNull
    @Override
    public EntityIdSet toSet(@NotNull final PersistentStoreTransaction txn) {
        return source.toSet(txn);
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return source.countImpl(txn);
    }

    @Override
    protected int indexOfImpl(@NotNull final EntityId entityId) {
        return source.indexOfImpl(entityId);
    }

    @Override
    @NotNull
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return source.getIteratorImpl(txn);
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return source.getHandleImpl();
    }
}
