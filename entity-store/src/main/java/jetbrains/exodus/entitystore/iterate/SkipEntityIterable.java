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

public class SkipEntityIterable extends EntityIterableDecoratorBase {

    private final int itemsToSkip;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new SkipEntityIterable(txn,
                        (EntityIterableBase) parameters[1], Integer.valueOf((String) parameters[0]));
            }
        });
    }

    protected SkipEntityIterable(@NotNull final PersistentStoreTransaction txn,
                                 @NotNull final EntityIterableBase source,
                                 final int itemsToSkip) {
        super(txn, source);
        this.itemsToSkip = itemsToSkip;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.SKIP;
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

            private EntityIteratorBase sourceIt = null;

            @Override
            protected boolean hasNextImpl() {
                skip();
                return sourceIt.hasNextImpl();
            }

            @Override
            public EntityId nextIdImpl() {
                skip();
                return sourceIt.nextIdImpl();
            }

            private void skip() {
                EntityIteratorBase sourceIt = this.sourceIt;
                if (sourceIt == null) {
                    this.sourceIt = sourceIt = (EntityIteratorBase) source.iterator();
                    sourceIt.skip(itemsToSkip);
                }
            }
        };
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), SkipEntityIterable.getType(), source.getHandle()) {

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(itemsToSkip);
                builder.append('-');
                applyDecoratedToBuilder(builder);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(itemsToSkip);
                hash.applyDelimiter();
                super.hashCode(hash);
            }
        };
    }
}
