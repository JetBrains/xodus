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

public class EntityIdArrayCachedInstanceIterableTests extends EntityStoreTestBase {

    public void testEmpty() {
        examine();
    }

    public void testNull() {
        examine(-1, 0);
    }

    public void testNullSorted() {
        examine(-1, 0, -1, 1);
    }

    public void testNullCompact() {
        examine(-1, 1, 2, 3);
    }

    public void testNullDiverse() {
        examineUnsorted(-1, 1, 2, 3, 2, 2);
    }

    public void testNullCompact2() {
        examineUnsorted(2, 3, -1, 1);
    }

    public void testNullDiverse2() {
        examineUnsorted(2, 3, 2, 2, -1, 1);
    }

    public void testSingle() {
        examine(0, 1, 0, 2);
    }

    public void testSkipCompact() {
        TestEntityIterableImpl t = t(0, 6, 0, 7, 0, 8, 0, 9, 1, 6, 1, 7, 1, 8, 1, 9);
        CachedInstanceIterable w = w(t);
        assertEquals(true, w.isSortedById());
        examineSkip(t, w, 1, 3);
        examineSkip(t, w, 2, 4);
        examineSkip(t, w, 3, 6);
        examineSkip(t, w, 5, 8);
    }

    public void testSkipCompact2() {
        TestEntityIterableImpl t = t(0, 6, 0, 7, 0, 8, 0, 9, 1, 6, 1, 7, 1, 8, 1, 9, 2, 1);
        CachedInstanceIterable w = w(t);
        assertEquals(true, w.isSortedById());
        examineSkip(t, w, 1, 3);
        examineSkip(t, w, 2, 4);
        examineSkip(t, w, 3, 6);
        examineSkip(t, w, 5, 8);
    }

    public void testSingleUnsorted() {
        examineUnsorted(0, 6, 0, 7, 0, 8, 0, 5, 0, 9);
    }

    public void testDiverseUnsorted() {
        examineUnsorted(0, 5, 0, 6, 1, 6, 1, 5, 1, 7, 1, 8);
    }

    public void testDiverseSorted() {
        examine(0, 5, 0, 6, 1, 5, 1, 6, 1, 7, 1, 8);
    }

    void examine(final long... ids) {
        TestEntityIterableImpl t = t(ids);
        CachedInstanceIterable w = w(t);
        assertTrue(w.isSortedById());
        assertIterablesMatch(t, w);
        t.isSortedById = false;
        w = w(t);
        assertTrue(w.isSortedById());
        assertIterablesMatch(t, w);
    }

    void examineUnsorted(final long... ids) {
        TestEntityIterableImpl t = t(false, ids);
        CachedInstanceIterable w = w(t);
        assertEquals(false, w.isSortedById());
        assertIterablesMatch(t, w);
    }

    void assertIterablesMatch(EntityIterableBase expected, CachedInstanceIterable actual) {
        assertEquals(expected.count(), actual.count());
        assertIteratorsMatch(expected.iterator(), actual.iterator());
        final PersistentStoreTransaction txn = getStoreTransaction();
        assertIteratorsMatch(expected.getReverseIteratorImpl(txn), actual.getReverseIteratorImpl(txn));
        int index = 0;
        for (Entity e : expected) {
            if (e != null) { //TODO: allow indexOf(null)
                assertEquals(index, actual.indexOf(e));
            }
            ++index;
        }
        assertEquals(-1, actual.indexOfImpl(new PersistentEntityId(239, 1)));
        EntityIdSet idSet = actual.toSet(txn);
        for (Entity e : expected) {
            assertTrue(idSet.contains(e == null ? null : e.getId()));
        }
        if (expected.isEmpty()) {
            assertEquals(null, ((EntityIteratorBase) actual.getIteratorImpl(txn)).getLast());
            assertEquals(null, ((EntityIteratorBase) actual.getReverseIteratorImpl(txn)).getLast());
        } else {
            final Entity last = expected.getReverseIteratorImpl(txn).next();
            assertEquals(last == null ? null : last.getId(), ((EntityIteratorBase) actual.getIteratorImpl(txn)).getLast());
            final Entity first = expected.getIteratorImpl(txn).next();
            assertEquals(first == null ? null : first.getId(), ((EntityIteratorBase) actual.getReverseIteratorImpl(txn)).getLast());
        }
    }

    void examineSkip(TestEntityIterableImpl t, CachedInstanceIterable w, int from, int to) {
        assertIteratorsMatch(t.iterator(), w.iterator(), from, to);
        final PersistentStoreTransaction txn = getStoreTransaction();
        assertIteratorsMatch(t.getReverseIteratorImpl(txn), w.getReverseIteratorImpl(txn), from, to);
    }

    static void assertIteratorsMatch(EntityIterator expected, EntityIterator actual) {
        while (expected.hasNext()) {
            assertTrue(actual.hasNext());
            assertEquals(expected.next(), actual.next());
        }
        assertFalse(actual.hasNext());
    }

    static void assertIteratorsMatch(EntityIterator expected, EntityIterator actual, int from, int to) {
        int i = 0;
        while (expected.hasNext()) {
            assertTrue(actual.hasNext());
            assertEquals(expected.next(), actual.next());
            if (++i == from) {
                final int length = to - from;
                assertEquals(expected.skip(length), actual.skip(length));
            }
        }
        assertFalse(actual.hasNext());
    }

    CachedInstanceIterable w(TestEntityIterableImpl t) {
        return EntityIdArrayCachedInstanceIterableFactory.createInstance(getStoreTransaction(), t);
    }

    TestEntityIterableImpl t(final boolean isSortedById, final long... ids) {
        return new TestEntityIterableImpl(isSortedById, pack(ids));
    }

    TestEntityIterableImpl t(final long... ids) {
        return new TestEntityIterableImpl(true, pack(ids));
    }

    static EntityId[] pack(final long... ids) {
        final int length = ids.length;
        final EntityId[] result = new EntityId[length / 2];
        int i = 0;
        while (i < length) {
            final int next = i + 1;
            final int typeId = (int) ids[i];
            result[i / 2] = typeId < 0 ? null : new PersistentEntityId(typeId, ids[next]);
            i = next + 1;
        }
        return result;
    }

    @SuppressWarnings({"AssignmentToCollectionOrArrayFieldFromParameter"})
    class TestEntityIterableImpl extends EntityIterableBase {
        boolean isSortedById;
        private final EntityId[] data;

        TestEntityIterableImpl(final boolean isSortedById, final EntityId[] data) {
            super(null);
            this.isSortedById = isSortedById;
            this.data = data;
        }

        @NotNull
        @Override
        public PersistentEntityStoreImpl getStore() {
            return getEntityStore();
        }

        @Override
        public boolean isSortedById() {
            return isSortedById;
        }

        @Override
        protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
            return data.length;
        }

        @Override
        protected int indexOfImpl(@NotNull EntityId entityId) {
            for (int i = 0; i < data.length; ++i) {
                if (entityId.equals(data[i])) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public EntityIteratorBase iterator() {
            return getIteratorImpl(getTransaction());
        }

        @NotNull
        @Override
        public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
            return new NonDisposableEntityIterator(this) {

                int i = 0;

                @Override
                public boolean skip(int number) {
                    i += number;
                    return hasNext();
                }

                @Override
                protected boolean hasNextImpl() {
                    return i < data.length;
                }

                @Nullable
                @Override
                protected EntityId nextIdImpl() {
                    final EntityId sourceId = data[i++];
                    return sourceId == null ? null : new PersistentEntityId(sourceId);
                }
            };
        }

        @NotNull
        @Override
        public EntityIteratorBase getReverseIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
            return new NonDisposableEntityIterator(this) {

                int i = data.length;

                @Override
                public boolean skip(int number) {
                    i -= number;
                    return hasNext();
                }

                @Override
                protected boolean hasNextImpl() {
                    return i > 0;
                }

                @Nullable
                @Override
                protected EntityId nextIdImpl() {
                    final EntityId sourceId = data[--i];
                    return sourceId == null ? null : new PersistentEntityId(sourceId);
                }
            };
        }

        @Override
        public boolean isEmpty() {
            return countImpl(getTransaction()) == 0;
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
        public long getRoughCount() {
            return countImpl(getTransaction());
        }

        @Override
        public int indexOf(@NotNull Entity entity) {
            return indexOfImpl(entity.getId());
        }

        @Override
        public boolean contains(@NotNull Entity entity) {
            return indexOf(entity) >= 0;
        }

        @Override
        @NotNull
        @SuppressWarnings("EmptyClass")
        public EntityIterableHandle getHandleImpl() {
            return new ConstantEntityIterableHandle(getEntityStore(), EntityIterableType.DISTINCT) {

                @Override
                public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                }
            };
        }

        @Override
        public boolean isSortResult() {
            return false;
        }

        @Override
        public boolean canBeCached() {
            return false;
        }

        @Override
        protected CachedInstanceIterable createCachedInstance(@NotNull final PersistentStoreTransaction txn) {
            return EntityIdArrayCachedInstanceIterableFactory.createInstance(txn, this);
        }

        // all following unsupported

        @Override
        @NotNull
        public EntityIterable intersect(@NotNull EntityIterable right) {
            throw new UnsupportedOperationException();
        }

        @Override
        @NotNull
        public EntityIterable intersectSavingOrder(@NotNull EntityIterable right) {
            throw new UnsupportedOperationException();
        }

        @Override
        @NotNull
        public EntityIterable union(@NotNull EntityIterable right) {
            throw new UnsupportedOperationException();
        }

        @Override
        @NotNull
        public EntityIterable minus(@NotNull EntityIterable right) {
            throw new UnsupportedOperationException();
        }

        @Override
        @NotNull
        public EntityIterable concat(@NotNull EntityIterable right) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public EntityIterable take(int number) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public EntityIterable asSortResult() {
            throw new UnsupportedOperationException();
        }
    }
}
