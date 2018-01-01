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

import jetbrains.exodus.entitystore.*;
import org.jetbrains.annotations.NotNull;

public class TakeEntityIterable extends EntityIterableDecoratorBase {

    private final int itemsToTake;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new TakeEntityIterable(txn,
                        (EntityIterableBase) parameters[1], Integer.valueOf((String) parameters[0]));
            }
        });
    }

    protected TakeEntityIterable(@NotNull final PersistentStoreTransaction txn,
                                 @NotNull final EntityIterableBase source,
                                 final int itemsToTake) {
        super(txn, source);
        this.itemsToTake = itemsToTake;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.TAKE;
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new NonDisposableEntityIterator(this) {

            @NotNull
            private final EntityIteratorBase sourceIt = (EntityIteratorBase) source.iterator();
            private int processed = 0;

            @Override
            protected boolean hasNextImpl() {
                return processed < itemsToTake && sourceIt.hasNextImpl();
            }

            @Override
            public EntityId nextIdImpl() {
                ++processed;
                return sourceIt.nextIdImpl();
            }
        };
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), TakeEntityIterable.getType(), source.getHandle()) {

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(itemsToTake);
                builder.append('-');
                applyDecoratedToBuilder(builder);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(itemsToTake);
                hash.applyDelimiter();
                super.hashCode(hash);
            }
        };
    }
}
