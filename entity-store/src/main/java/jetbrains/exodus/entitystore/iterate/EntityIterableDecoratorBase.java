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
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"ProtectedField"})
public abstract class EntityIterableDecoratorBase extends EntityIterableBase {

    @NotNull
    protected final EntityIterableBase source;


    protected EntityIterableDecoratorBase(@NotNull final PersistentStoreTransaction txn,
                                          @NotNull final EntityIterableBase source) {
        super(txn);
        this.source = source.getSource();
        this.txnGetter = source.txnGetter;
    }

    @Override
    public boolean setOrigin(Object origin) {
        if (super.setOrigin(origin)) {
            source.setOrigin(origin);
            return true;
        }
        return false;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    public int depth() {
        return source.depth() + 1;
    }

    @Override
    public boolean canBeCached() {
        // this is actually overkill until decorator has a txn itself
        return super.canBeCached() && (source.canBeCached() || source.isCachedInstance());
    }

    @Override
    public boolean isThreadSafe() {
        return super.isThreadSafe() && source.isThreadSafe();
    }

    @NotNull
    public final EntityIterableBase getDecorated() {
        return source;
    }
}
