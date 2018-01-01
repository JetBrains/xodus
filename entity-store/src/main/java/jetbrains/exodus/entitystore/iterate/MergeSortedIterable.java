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
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

@Deprecated
@SuppressWarnings({"AssignmentToCollectionOrArrayFieldFromParameter"})
public class MergeSortedIterable extends EntityIterableBase {

    @NotNull
    private final List<EntityIterable> sorted;
    @NotNull
    private final Comparator<Entity> comparator;

    static {
        /*
        Attention!! Comparator is fake. Should work for iteration count, but not for actual results.
        */
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                int size = Integer.valueOf((String) parameters[0]);
                ArrayList<EntityIterable> sorted = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    sorted.add((EntityIterable) parameters[i + 1]);
                }
                return new MergeSortedIterable(txn, sorted, new Comparator<Entity>() {
                    @Override
                    public int compare(Entity o1, Entity o2) {
                        return o1.getId().compareTo(o2.getId());
                    }
                });
            }
        });
    }

    public MergeSortedIterable(@Nullable final PersistentStoreTransaction txn,
                               @NotNull final List<EntityIterable> sorted,
                               @NotNull final Comparator<Entity> comparator) {
        super(txn);
        this.sorted = sorted;
        this.comparator = comparator;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.MERGE_SORTED;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        long result = 0;
        for (final EntityIterable it : sorted) {
            result += ((EntityIterableBase) it).getSource().countImpl(txn);
        }
        return result;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new MergeSortedIterator();
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleBase(getStore(), MergeSortedIterable.getType()) {
            @Override
            public boolean isMatchedLinkAdded(@NotNull EntityId source, @NotNull EntityId target, int linkId) {
                return false;
            }

            @Override
            public boolean isMatchedLinkDeleted(@NotNull EntityId source, @NotNull EntityId target, int linkId) {
                return false;
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(sorted.size());
                for (final EntityIterable it : sorted) {
                    builder.append('-');
                    ((EntityIterableHandleBase) ((EntityIterableBase) it).getSource().getHandle()).toString(builder);
                }
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(sorted.size());
                for (final EntityIterable it : sorted) {
                    hash.applyDelimiter();
                    hash.apply(((EntityIterableBase) it).getSource().getHandle());
                }
            }
        };
    }

    private final class MergeSortedIterator extends NonDisposableEntityIterator {

        private final PriorityQueue<EntityWithSource> queue;

        @SuppressWarnings({"ObjectAllocationInLoop"})
        private MergeSortedIterator() {
            super(MergeSortedIterable.this);
            queue = new PriorityQueue<>(sorted.size(), new Comparator<EntityWithSource>() {
                @Override
                public int compare(EntityWithSource o1, EntityWithSource o2) {
                    final Entity e1 = getEntity(o1.id);
                    final Entity e2 = getEntity(o2.id);
                    return comparator.compare(e1, e2);
                }
            });
            for (final EntityIterable it : sorted) {
                final EntityIterator i = it.iterator();
                if (i.hasNext()) {
                    final EntityId id = i.nextId();
                    if (id != null) {
                        queue.add(new EntityWithSource(id, i));
                    }
                }
            }
        }

        @Override
        protected boolean hasNextImpl() {
            return !queue.isEmpty();
        }

        @Override
        public EntityId nextIdImpl() {
            final EntityWithSource pair = queue.poll();
            final EntityId result = pair.id;
            final EntityIterator i = pair.source;
            if (i.hasNext()) {
                queue.offer(new EntityWithSource(i.nextId(), i));
            }
            return result;
        }

        private final class EntityWithSource {

            private final EntityId id;
            private final EntityIterator source;

            private EntityWithSource(EntityId id, EntityIterator source) {
                this.id = id;
                this.source = source;
            }
        }


    }

}
