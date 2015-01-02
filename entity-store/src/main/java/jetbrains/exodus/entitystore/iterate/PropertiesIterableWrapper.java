/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.PropertyTypes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

@SuppressWarnings({"RawUseOfParameterizedType", "ComparableImplementedButEqualsNotOverridden", "unchecked"})
public class PropertiesIterableWrapper extends CachedWrapperIterable {

    private static final Log log = LogFactory.getLog(PropertiesIterableWrapper.class);

    private int entityTypeId;
    @NotNull
    private final Persistent23Tree<IndexEntry> index;
    @Nullable
    private Persistent23Tree.MutableTree<IndexEntry> mutableIndex;
    @Nullable
    private Class<? extends Comparable> valueClass;

    @SuppressWarnings({"ConstantConditions", "ObjectAllocationInLoop"})
    public PropertiesIterableWrapper(@Nullable final PersistentEntityStoreImpl store,
                                     @Nullable final PropertyValueIterator it,
                                     @NotNull final EntityIterableBase source) {
        super(store, source);
        index = new Persistent23Tree<IndexEntry>();
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
                final Collection<IndexEntry> tempList = new ArrayList<IndexEntry>();
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
    private PropertiesIterableWrapper(@NotNull final PropertiesIterableWrapper source) {
        super(source.getStore(), source);
        entityTypeId = source.entityTypeId;
        index = source.index.getClone();
        mutableIndex = index.beginWrite();
        valueClass = source.valueClass;
    }

    @Nullable
    public Class<? extends Comparable> getPropertyValueClass() {
        return valueClass;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    public PropertiesIterableWrapper beginUpdate() {
        return new PropertiesIterableWrapper(this);
    }

    public void endUpdate() {
        Persistent23Tree.MutableTree<IndexEntry> index = mutableIndex;
        if (index == null) {
            throw new IllegalStateException("Index was not mutated");
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
                log.warn("In-memory index doesn't contain the value [" + oldValue + "]. New value [" + newValue + "]. Handle [" + getHandle().getStringHandle() + ']');
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
        return new PropertyValueIteratorWrapper(value);
    }

    EntityIteratorBase getPropertyRangeIterator(@NotNull final Comparable min, @NotNull final Comparable max) {
        return new PropertyRangeIteratorWrapper(min, max);
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return index.getCurrent().isEmpty() ? EntityIteratorBase.EMPTY : new PropertiesIteratorWrapper();
    }

    @Override
    @NotNull
    public EntityIteratorBase getReverseIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return index.getCurrent().isEmpty() ? EntityIteratorBase.EMPTY : new ReversePropertiesIteratorWrapper();
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return index.getCurrent().size();
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
    private abstract class PropertiesIteratorWrapperBase extends NonDisposableEntityIterator implements PropertyValueIterator {

        @NotNull
        private final Iterator<IndexEntry> it;
        @Nullable
        protected IndexEntry next;
        protected boolean hasNextValid;

        protected PropertiesIteratorWrapperBase(@NotNull final Iterator<IndexEntry> it) {
            super(PropertiesIterableWrapper.this);
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

    private class PropertiesIteratorWrapper extends PropertiesIteratorWrapperBase {

        private PropertiesIteratorWrapper() {
            super(index.getCurrent().iterator());
        }
    }

    private class ReversePropertiesIteratorWrapper extends PropertiesIteratorWrapperBase {

        private ReversePropertiesIteratorWrapper() {
            super(index.getCurrent().reverseIterator());
        }
    }

    private class PropertyValueIteratorWrapper extends PropertiesIteratorWrapperBase {

        @NotNull
        private final Comparable value;

        private PropertyValueIteratorWrapper(@NotNull final Comparable value) {
            super(index.getCurrent().tailIterator(new IndexEntry(value, 0)));
            this.value = value;
        }

        @Override
        protected boolean checkIndexEntry(final IndexEntry entry) {
            return entry != null && entry.propValue.compareTo(value) == 0;
        }
    }

    private class PropertyRangeIteratorWrapper extends PropertiesIteratorWrapperBase {

        @NotNull
        private final Comparable max;

        private PropertyRangeIteratorWrapper(@NotNull final Comparable min, @NotNull final Comparable max) {
            super(index.getCurrent().tailIterator(new IndexEntry(min, 0)));
            this.max = max;
        }

        @Override
        protected boolean checkIndexEntry(final IndexEntry entry) {
            return entry != null && entry.propValue.compareTo(max) <= 0;
        }
    }
}
