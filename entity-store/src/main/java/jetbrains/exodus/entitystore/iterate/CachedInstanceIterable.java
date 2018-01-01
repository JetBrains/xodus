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

import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class CachedInstanceIterable extends EntityIterableBase {
    @NotNull
    private final EntityIterableHandle sourceHandle;

    protected CachedInstanceIterable(@Nullable final PersistentStoreTransaction txn,
                                     @NotNull final EntityIterableBase source) {
        super(txn);
        sourceHandle = source.getHandle();
        txnGetter = source.txnGetter;
    }

    @Override
    public EntityIterator iterator() {
        return getIteratorImpl();
    }

    @Override
    public boolean nonCachedHasFastCountAndIsEmpty() {
        return true;
    }

    @Override
    public long size() {
        return countImpl(getTransaction());
    }

    @Override
    public long count() {
        return countImpl(getTransaction());
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return sourceHandle;
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    @Override
    public boolean isCachedInstance() {
        return true;
    }

    public boolean isUpdatable() {
        return false;
    }

    @Override
    public boolean isEmptyImpl(@NotNull final PersistentStoreTransaction txn) {
        return countImpl(txn) == 0;
    }

    protected abstract CachedInstanceIterable orderById();
}
