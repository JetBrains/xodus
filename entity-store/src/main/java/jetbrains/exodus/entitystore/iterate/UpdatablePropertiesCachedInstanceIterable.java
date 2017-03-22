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

import jetbrains.exodus.core.dataStructures.persistent.AbstractPersistent23Tree;
import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityStoreException;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.tables.PropertyTypes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

@SuppressWarnings({"RawUseOfParameterizedType", "ComparableImplementedButEqualsNotOverridden", "unchecked"})
public class UpdatablePropertiesCachedInstanceIterable extends UpdatableCachedInstanceIterable {

    private static final Logger logger = LoggerFactory.getLogger(UpdatablePropertiesCachedInstanceIterable.class);

    private int entityTypeId;
    @NotNull
    private final Persistent23Tree<IndexEntry> index;
    @Nullable
    private Persistent23Tree.MutableTree<IndexEntry> mutableIndex;
    @Nullable
    private Class<? extends Comparable> valueClass;

    @SuppressWarnings({"ConstantConditions", "ObjectAllocationInLoop"})
    public UpdatablePropertiesCachedInstanceIterable(@Nullable final PersistentStoreTransaction txn,
                                                     @Nullable final PropertyValueIterator it,
                                                     @NotNull final EntityIterableBase source) {
        super(txn, source);
        index = new Persistent23Tree<>();
        mutableIndex = null;
        if (it == null) {
            entityTypeId = -1;
            return;
        }
        try {
            if (!it.hasNext()) {
                entityTypeId = -1;
                valueClass = null;
            } else {
                final Collection<IndexEntry> tempList = new ArrayList<>();
                EntityId id = it.nextId();
                entityTypeId = id.getTypeId();
                Comparable propValue = it.currentValue();
                valueClass = propValue.getClass();
                while (true) {
                    tempList.add(new IndexEntry(propValue, id.getLocalId()));
                    if (!it.hasNext()) {
                        break;
                    }
                    id = it.nextId();
                    propValue = it.currentValue();
                    if (!valueClass.equals(propValue.getClass())) {
                        throw new EntityStoreException("Unexpected property value class");
                    }
                }
                final Persistent23Tree.MutableTree<IndexEntry> mutableTree = index.beginWrite();
                mutableTree.addAll(tempList, tempList.size());
                mutableTree.endWrite();
            }
        } finally {
            ((EntityIteratorBase) it).disposeIfShouldBe();
        }
    }

    // constructor for mutating source index
    private UpdatablePropertiesCachedInstanceIterable(@NotNull final UpdatablePropertiesCachedInstanceIterable source) {
        super(source.getTransaction(), source);
        entityTypeId = source.entityTypeId;
        index = source.index.getClone();
        mutableIndex = index.beginWrite();
        valueClass = source.valueClass;
    }

    @Override
    public int getEntityTypeId() {
        return entityTypeId;
    }

    @Nullable
    public Class<? extends Comparable> getPropertyValueClass() {
        return valueClass;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    public UpdatablePropertiesCachedInstanceIterable beginUpdate() {
        return new UpdatablePropertiesCachedInstanceIterable(this);
    }

    @Override
    public boolean isMutated() {
        return mutableIndex != null;
    }

    public void endUpdate() {
        Persistent23Tree.MutableTree<IndexEntry> index = mutableIndex;
        if (index == null) {
            throw new IllegalStateException("UpdatablePropertiesCachedInstanceIterable was not mutated");
        }
        index.endWrite();
        mutableIndex = null;
    }

    public void update(final int typeId, final long localId,
                       @Nullable final Comparable oldValue, @Nullable final Comparable newValue) {
        if (entityTypeId == -1) {
            entityTypeId = typeId;
        }
        final IndexEntry oldEntry = oldValue == null ? null : new IndexEntry(PropertyTypes.toLowerCase(oldValue), localId);
        final IndexEntry newEntry = newValue == null ? null : new IndexEntry(PropertyTypes.toLowerCase(newValue), localId);
        if (oldEntry == null && newEntry == null) {
            throw new IllegalStateException("Can't update in-memory index: both oldValue and newValue are null");
        }
        final Persistent23Tree.MutableTree<IndexEntry> index = mutableIndex;
        if (index == null) {
            throw new IllegalStateException("Mutate index before updating it");
        }
        if (oldEntry != null) {
            if (index.contains(oldEntry)) {
                index.exclude(oldEntry);
            } else if (newEntry != null && !index.contains(newEntry)) {
                logger.warn("In-memory index doesn't contain the value [" + oldValue + "]. New value [" + newValue + "]. Handle [" + getHandle() + ']');
            }
        }
        if (newEntry != null) {
            index.add(newEntry);
            if (valueClass == null) {
                valueClass = newValue.getClass();
            }
        }
    }

    EntityIteratorBase getPropertyValueIterator(@NotNull final Comparable value) {
        return new PropertyValueCachedInstanceIterator(value);
    }

    EntityIteratorBase getPropertyRangeIterator(@NotNull final Comparable min, @NotNull final Comparable max) {
        return new PropertyRangeCachedInstanceIterator(min, max);
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return getCurrentTree().isEmpty() ? EntityIteratorBase.EMPTY : new PropertiesCachedInstanceIterator();
    }

    @Override
    @NotNull
    public EntityIteratorBase getReverseIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return getCurrentTree().isEmpty() ? EntityIteratorBase.EMPTY : new ReversePropertiesCachedInstanceIterator();
    }

    @Override
    public long size() {
        return getCurrentTree().size();
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return size();
    }

    private AbstractPersistent23Tree<IndexEntry> getCurrentTree() {
        return mutableIndex == null ? index.beginRead() : mutableIndex;
    }

    /**
     * Index key.
     */

    private static class IndexEntry implements Comparable<IndexEntry> {

        @NotNull
        private final Comparable propValue;
        private final long localId;

        private IndexEntry(@NotNull final Comparable propValue, final long localId) {
            this.propValue = propValue;
            this.localId = localId;
        }

        @Override
        public int compareTo(IndexEntry o) {
            int result = propValue.compareTo(o.propValue);
            if (result == 0) {
                if (localId < o.localId) {
                    result = -1;
                } else if (localId > o.localId) {
                    result = 1;
                }
            }
            return result;
        }
    }

    /**
     * Iterators over index.
     */

    @SuppressWarnings({"PackageVisibleField", "ProtectedField"})
    private abstract class PropertiesCachedInstanceIteratorBase extends NonDisposableEntityIterator implements PropertyValueIterator {

        @NotNull
        private final Iterator<IndexEntry> it;
        @Nullable
        protected IndexEntry next;
        protected boolean hasNextValid;

        protected PropertiesCachedInstanceIteratorBase(@NotNull final Iterator<IndexEntry> it) {
            super(UpdatablePropertiesCachedInstanceIterable.this);
            this.it = it;
        }

        @Override
        protected boolean hasNextImpl() {
            if (hasNextValid) {
                return true;
            }
            if (!it.hasNext()) {
                return false;
            }
            final IndexEntry next = it.next();
            if (!checkIndexEntry(next)) {
                return false;
            }
            this.next = next;
            hasNextValid = true;
            return true;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (!hasNextImpl()) {
                return null;
            }
            //noinspection ConstantConditions
            EntityId result = new PersistentEntityId(entityTypeId, next.localId);
            hasNextValid = false;
            return result;
        }

        @Override
        @Nullable
        public Comparable currentValue() {
            final IndexEntry entry = next;
            return entry == null ? null : entry.propValue;
        }

        protected boolean checkIndexEntry(final IndexEntry entry) {
            return entry != null;
        }
    }

    private class PropertiesCachedInstanceIterator extends PropertiesCachedInstanceIteratorBase {

        private PropertiesCachedInstanceIterator() {
            super(getCurrentTree().iterator());
        }
    }

    private class ReversePropertiesCachedInstanceIterator extends PropertiesCachedInstanceIteratorBase {

        private ReversePropertiesCachedInstanceIterator() {
            super(getCurrentTree().reverseIterator());
        }
    }

    private class PropertyValueCachedInstanceIterator extends PropertiesCachedInstanceIteratorBase {

        @NotNull
        private final Comparable value;

        private PropertyValueCachedInstanceIterator(@NotNull final Comparable value) {
            super(getCurrentTree().tailIterator(new IndexEntry(value, 0)));
            this.value = value;
        }

        @Override
        protected boolean checkIndexEntry(final IndexEntry entry) {
            return entry != null && entry.propValue.compareTo(value) == 0;
        }
    }

    private class PropertyRangeCachedInstanceIterator extends PropertiesCachedInstanceIteratorBase {

        @NotNull
        private final Comparable max;

        private PropertyRangeCachedInstanceIterator(@NotNull final Comparable min, @NotNull final Comparable max) {
            super(getCurrentTree().tailIterator(new IndexEntry(min, 0)));
            this.max = max;
        }

        @Override
        protected boolean checkIndexEntry(final IndexEntry entry) {
            return entry != null && entry.propValue.compareTo(max) <= 0;
        }
    }
}
