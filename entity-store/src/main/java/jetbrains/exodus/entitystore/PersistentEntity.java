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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.util.StringBuilderSpinAllocator;
import jetbrains.exodus.util.StringInterner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

@SuppressWarnings({"unchecked"})
public class PersistentEntity implements Entity, TxnProvider {

    @NotNull
    private final PersistentEntityStoreImpl store;
    @NotNull
    private final PersistentEntityId id;

    public PersistentEntity(@NotNull final PersistentEntityStoreImpl store, @NotNull final PersistentEntityId id) {
        this.store = store;
        this.id = id;
    }

    public ReadOnlyPersistentEntity getSnapshot(@NotNull final PersistentStoreTransaction txn) {
        if (txn.store != store) {
            throw new IllegalArgumentException("Can't get entity snapshot against transaction from another store!");
        }
        return new ReadOnlyPersistentEntity(txn, id);
    }

    @NotNull
    public PersistentStoreTransaction getTransaction() {
        return store.getAndCheckCurrentTransaction();
    }

    protected void assertWritable() {
    }

    @Override
    public String toString() {
        final StringBuilder builder = StringBuilderSpinAllocator.alloc();
        try {
            builder.append(getType());
            builder.append(": id = ");
            id.toString(builder);
            return builder.toString();
        } finally {
            StringBuilderSpinAllocator.dispose(builder);
        }
    }

    @Override
    @NotNull
    public EntityStore getStore() {
        return store;
    }

    @Override
    @NotNull
    public PersistentEntityId getId() {
        return id;
    }

    @Override
    @NotNull
    public String toIdString() {
        return id.toString();
    }

    @Override
    @NotNull
    public String getType() {
        return store.getEntityType(this, id.getTypeId());
    }

    @Override
    public boolean delete() {
        assertWritable();
        return store.deleteEntity(getTransaction(), this);
    }

    @Override
    @Nullable
    public ByteIterable getRawProperty(@NotNull final String propertyName) {
        final PersistentStoreTransaction txn = getTransaction();
        final int propertyId = store.getPropertyId(txn, propertyName, false);
        return propertyId < 0 ? null : store.getRawProperty(txn, id, propertyId);
    }

    @Override
    @Nullable
    public Comparable getProperty(@NotNull final String propertyName) {
        return store.getProperty(getTransaction(), this, propertyName);
    }

    @Override
    public boolean setProperty(@NotNull final String propertyName, @NotNull final Comparable value) {
        assertWritable();
        return store.setProperty(getTransaction(), this, propertyName, value);
    }

    @Override
    public boolean deleteProperty(@NotNull final String propertyName) {
        assertWritable();
        return store.deleteProperty(getTransaction(), this, propertyName);
    }

    @Override
    @NotNull
    public List<String> getPropertyNames() {
        return store.getPropertyNames(getTransaction(), this);
    }

    public boolean hasBlob(@NotNull final String blobName) {
        try {
            return store.getBlobHandleAndValue(getTransaction(), this, blobName) != null;
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    @Nullable
    public InputStream getBlob(@NotNull final String blobName) {
        try {
            return store.getBlob(getTransaction(), this, blobName);
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    public long getBlobSize(@NotNull String blobName) {
        try {
            return store.getBlobSize(getTransaction(), this, blobName);
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    @Nullable
    public String getBlobString(@NotNull final String blobName) {
        try {
            return store.getBlobString(getTransaction(), this, blobName);
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    public void setBlob(@NotNull final String blobName, @NotNull final InputStream blob) {
        assertWritable();
        try {
            store.setBlob(getTransaction(), this, blobName, blob);
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    public void setBlob(@NotNull final String blobName, @NotNull final File file) {
        assertWritable();
        try {
            store.setBlob(getTransaction(), this, blobName, file);
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
    }

    @Override
    public boolean setBlobString(@NotNull final String blobName, @NotNull final String blobString) {
        String oldValue = getBlobString(blobName);
        if (blobString.equals(oldValue)) {
            return false;
        }
        try {
            store.setBlobString(getTransaction(), this, blobName, blobString);
        } catch (Exception e) {
            throw ExodusException.toEntityStoreException(e);
        }
        return true;
    }

    @Override
    public boolean deleteBlob(@NotNull final String blobName) {
        assertWritable();
        return store.deleteBlob(getTransaction(), this, blobName);
    }

    @Override
    @NotNull
    public List<String> getBlobNames() {
        return store.getBlobNames(getTransaction(), this);
    }

    @Override
    public boolean addLink(@NotNull final String linkName, @NotNull final Entity target) {
        assertWritable();
        final PersistentStoreTransaction txn = getTransaction();
        final int linkId = store.getLinkId(txn, linkName, true);
        return store.addLink(txn, this, (PersistentEntity) target, linkId);
    }

    @Override
    @Nullable
    public Entity getLink(@NotNull final String linkName) {
        final PersistentStoreTransaction txn = getTransaction();
        final int linkId = store.getLinkId(txn, linkName, false);
        return linkId < 0 ? null : store.getLink(txn, this, linkId);
    }

    @Override
    public boolean setLink(@NotNull final String linkName, @Nullable final Entity target) {
        assertWritable();
        final PersistentStoreTransaction txn = getTransaction();
        final int linkId = store.getLinkId(txn, linkName, true);
        return store.setLink(txn, this, linkId, (PersistentEntity) target);
    }

    @Override
    @NotNull
    public EntityIterable getLinks(@NotNull final String linkName) {
        final PersistentStoreTransaction txn = getTransaction();
        final int linkId = store.getLinkId(txn, linkName, false);
        if (linkId < 0) {
            return EntityIterableBase.EMPTY;
        }
        return store.getLinks(txn, this, linkId);
    }

    @Override
    @NotNull
    public EntityIterable getLinks(@NotNull final Collection<String> linkNames) {
        final IntHashMap<String> linkIds = new IntHashMap<>(linkNames.size());
        final PersistentStoreTransaction txn = getTransaction();
        for (final String linkName : linkNames) {
            final int linkId = store.getLinkId(txn, linkName, false);
            if (linkId >= 0) {
                linkIds.put(linkId, StringInterner.intern(linkName));
            }
        }
        return store.getLinks(txn, this, linkIds);
    }

    @Override
    public boolean deleteLink(@NotNull final String linkName, @NotNull final Entity target) {
        assertWritable();
        final PersistentStoreTransaction txn = getTransaction();
        final int linkId = store.getLinkId(txn, linkName, false);
        if (linkId < 0) {
            return false;
        }
        return store.deleteLink(txn, this, linkId, (PersistentEntity) target);
    }

    @Override
    public void deleteLinks(@NotNull final String linkName) {
        assertWritable();
        final PersistentStoreTransaction txn = getTransaction();
        final int linkId = store.getLinkId(txn, linkName, false);
        if (linkId < 0) {
            return;
        }
        final EntityIterator itr = store.getLinks(txn, this, linkId).iterator();
        while (itr.hasNext()) {
            store.deleteLink(txn, this, linkId, (PersistentEntityId) itr.nextId());
        }
    }

    @Override
    @NotNull
    public List<String> getLinkNames() {
        return store.getLinkNames(getTransaction(), this);
    }

    public int hashCode() {
        return store.hashCode() + id.hashCode();
    }

    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PersistentEntity)) {
            return false;
        }
        final PersistentEntity that = (PersistentEntity) obj;
        return id.equals(that.id) && store == that.store;
    }

    @Override
    public int compareTo(@NotNull final Entity o) {
        return id.compareTo(o.getId());
    }
}
