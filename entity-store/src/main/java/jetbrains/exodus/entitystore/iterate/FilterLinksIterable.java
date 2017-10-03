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
import org.jetbrains.annotations.Nullable;

public class FilterLinksIterable extends EntityIterableDecoratorBase {

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new FilterLinksIterable(txn,
                    Integer.valueOf((String) parameters[0]), (EntityIterableBase) parameters[1], (EntityIterable) parameters[2]);
            }
        });
    }

    private final int linkId;
    @NotNull
    private final EntityIterableBase entities;

    public FilterLinksIterable(@NotNull final PersistentStoreTransaction txn,
                               final int linkId,
                               @NotNull final EntityIterableBase source,
                               @NotNull final EntityIterable entities) {
        super(txn, source);
        this.linkId = linkId;
        this.entities = ((EntityIterableBase) entities).getSource();
    }

    public static EntityIterableType getType() {
        return EntityIterableType.FILTER_LINKS;
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, new NonDisposableEntityIterator(this) {

            @NotNull
            private final EntityIteratorBase sourceIt = (EntityIteratorBase) source.iterator();
            @Nullable
            private EntityId nextId = PersistentEntityId.EMPTY_ID;
            @Nullable
            private EntityIdSet idSet = null;
            @NotNull
            private final PersistentEntityStoreImpl store = FilterLinksIterable.this.getStore();

            @Override
            protected boolean hasNextImpl() {
                if (nextId != PersistentEntityId.EMPTY_ID) {
                    return true;
                }
                while (sourceIt.hasNext()) {
                    nextId = sourceIt.nextId();
                    if (nextId != null) {
                        final PersistentEntityId targetId = store.getRawLinkAsEntityId(txn, (PersistentEntityId) nextId, linkId);
                        if (targetId != null && getIdSet().contains(targetId)) {
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            protected EntityId nextIdImpl() {
                final EntityId result = nextId;
                nextId = PersistentEntityId.EMPTY_ID;
                return result;
            }

            @NotNull
            private EntityIdSet getIdSet() {
                if (idSet == null) {
                    idSet = entities.toSet(txn);
                }
                return idSet;
            }
        });
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), getType(), source.getHandle()) {

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(linkId);
                builder.append('-');
                applyDecoratedToBuilder(builder);
                builder.append('-');
                ((EntityIterableHandleBase) entities.getHandle()).toString(builder);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(linkId);
                hash.applyDelimiter();
                super.hashCode(hash);
                hash.applyDelimiter();
                hash.apply(entities.getHandle());
            }
        };
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    @Override
    public boolean canBeCached() {
        return false;
    }
}
