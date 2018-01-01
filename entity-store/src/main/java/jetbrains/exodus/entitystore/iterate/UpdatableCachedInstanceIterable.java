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

import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.Updatable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class UpdatableCachedInstanceIterable extends CachedInstanceIterable implements Updatable {

    protected UpdatableCachedInstanceIterable(@Nullable final PersistentStoreTransaction txn,
                                              @NotNull final EntityIterableBase source) {
        super(txn, source);
    }

    @Override
    protected CachedInstanceIterable orderById() {
        throw new UnsupportedOperationException();
    }

    public boolean isUpdatable() {
        return true;
    }

    @Override
    public Updatable beginUpdate(@NotNull PersistentStoreTransaction txn) {
        final UpdatableCachedInstanceIterable result = beginUpdate();
        txn.registerMutatedHandle(getHandle(), result);
        return result;
    }

    public abstract UpdatableCachedInstanceIterable beginUpdate();

    public abstract boolean isMutated();

    @Override
    public void endUpdate(@NotNull PersistentStoreTransaction txn) {
        endUpdate();
        txn.getStore().getEntityIterableCache().setCachedCount(getHandle(), size());
    }

    public abstract void endUpdate();
}
