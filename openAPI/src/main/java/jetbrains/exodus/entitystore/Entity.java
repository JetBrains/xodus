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
import jetbrains.exodus.bindings.ComparableBinding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

/**
 * {@code Entity} is an object in the {@linkplain EntityStore}. Any {@code Entity} has {@linkplain #getType() type} and
 * {@linkplain #getId() unique id}. {@code Entity} can have {@linkplain #getProperty(String) properties} and
 * {@linkplain #getBlob(String) blobs}, and can be {@linkplain #getLink(String)  linked}. Each property, blob or link
 * is identified by its name. Although entity properties are expected to be {@code Comparable}, only Java primitives
 * types, {@linkplain String Strings} and {@linkplain jetbrains.exodus.bindings.ComparableSet ComparableSet} values can
 * be used by default. Use {@linkplain PersistentEntityStore#registerCustomPropertyType(StoreTransaction, Class,
 * ComparableBinding) registerCustomPropertyType()} to define your own property type.
 *
 * <p>To create new entity, use {@linkplain StoreTransaction#newEntity(String)}:
 * <pre>
 *     final Entity user = txn.newEntity("User");
 * </pre>
 * To get unique if of and entity, use {@linkplain #getId()}:
 * <pre>
 *     final EntityId id = user.getId();
 * </pre>
 * To get existing entity by id, use {@linkplain StoreTransaction#getEntity(EntityId)}:
 * <pre>
 *     final Entity user = txn.getEntity(id);
 * </pre>
 *
 * @see EntityStore
 * @see EntityId
 * @see StoreTransaction
 */
public interface Entity extends Comparable<Entity> {

    /**
     * @return instance of {@linkplain EntityStore} against which the entity was created
     */
    @NotNull
    EntityStore getStore();

    /**
     * @return {@linkplain EntityId unique id} of the entity
     */
    @NotNull
    EntityId getId();

    /**
     * @return string representation of the entity {@linkplain EntityId unique id}.
     * @see StoreTransaction#toEntityId(String)
     */
    @NotNull
    String toIdString();

    /**
     * @return entity type
     */
    @NotNull
    String getType();

    /**
     * Deletes the entity unconditionally.
     *
     * @return {@code true} if the entity was deleted, otherwise it didn't exist
     */
    boolean delete();

    /**
     * Gets {@linkplain ByteIterable raw value} of the property with specified name.
     *
     * @param propertyName name of the property
     * @return {@linkplain ByteIterable property raw value}, or {@code null} if the property is not set
     * @see ByteIterable
     * @see #getProperty(String)
     * @see #setProperty(String, Comparable)
     * @see #deleteProperty(String)
     * @see #getPropertyNames()
     */
    @Nullable
    ByteIterable getRawProperty(@NotNull final String propertyName);

    /**
     * Gets value of property with specified name.
     *
     * @param propertyName name of the property
     * @return property value, or {@code null} if the property is not set
     * @see #getRawProperty(String)
     * @see #setProperty(String, Comparable)
     * @see #deleteProperty(String)
     * @see #getPropertyNames()
     */
    @Nullable
    Comparable getProperty(@NotNull final String propertyName);

    /**
     * Sets property with specified name to specified value.
     *
     * @param propertyName name of the property
     * @param value        property value
     * @return {@code true} if value was actually set, i.e. if it is not equal to the old value
     * @see #getProperty(String)
     * @see #getRawProperty(String)
     * @see #deleteProperty(String)
     * @see #getPropertyNames()
     */
    boolean setProperty(@NotNull final String propertyName, @NotNull final Comparable value);

    /**
     * Deletes property with specified name.
     *
     * @param propertyName name of the property
     * @return {@code true} if the property was actually deleted, otherwise it didn't exist
     * @see #getProperty(String)
     * @see #getRawProperty(String)
     * @see #setProperty(String, Comparable)
     * @see #getPropertyNames()
     */
    boolean deleteProperty(@NotNull final String propertyName);

    /**
     * Returns names of all properties of the entity.
     *
     * @return {@linkplain List list} of property names
     * @see #getProperty(String)
     * @see #getRawProperty(String)
     * @see #setProperty(String, Comparable)
     * @see #deleteProperty(String)
     */
    @NotNull
    List<String> getPropertyNames();

    /**
     * Gets value of blob with specified name as {@linkplain InputStream}. The {@code InputStream} should be closed.
     *
     * @param blobName name of the blob
     * @return blob stream, or {@code null} if the blob is not set
     * @see #getBlobSize(String)
     * @see #getBlobString(String)
     * @see #setBlob(String, InputStream)
     * @see #setBlob(String, File)
     * @see #setBlobString(String, String)
     * @see #deleteBlob(String)
     */
    @Nullable
    InputStream getBlob(@NotNull final String blobName);

    /**
     * Gets the size of blob with specified name.
     *
     * @param blobName name of the blob
     * @return blob size, or negative value if the blob is not set
     * @see #getBlob(String)
     * @see #getBlobString(String)
     * @see #setBlob(String, InputStream)
     * @see #setBlob(String, File)
     * @see #setBlobString(String, String)
     * @see #deleteBlob(String)
     */
    long getBlobSize(@NotNull final String blobName);

    /**
     * Gets value of blob with specified name as
     * {@linkplain jetbrains.exodus.util.UTFUtil#readUTF(InputStream) UTF8-encoded string}.
     *
     * @param blobName name of the blob
     * @return blob string, or {@code null} if the blob is not set
     * @see #getBlob(String)
     * @see #getBlobSize(String)
     * @see jetbrains.exodus.util.UTFUtil#readUTF(InputStream)
     * @see #setBlob(String, InputStream)
     * @see #setBlob(String, File)
     * @see #setBlobString(String, String)
     * @see #deleteBlob(String)
     */
    @Nullable
    String getBlobString(@NotNull final String blobName);

    /**
     * Sets value (as {@linkplain InputStream}) of blob with specified name. For large blobs (having size greater
     * than {@linkplain PersistentEntityStoreConfig#getMaxInPlaceBlobSize()} which is by default 10000), it is
     * preferable to use {@linkplain #setBlob(String, File)} instead.
     *
     * @param blobName name of the blob
     * @param blob     blob value as {@linkplain InputStream}
     * @see #getBlob(String)
     * @see #getBlobSize(String)
     * @see #getBlobString(String)
     * @see #setBlob(String, File)
     * @see #setBlobString(String, String)
     * @see #deleteBlob(String)
     * @see PersistentEntityStoreConfig#getMaxInPlaceBlobSize()
     */
    void setBlob(@NotNull final String blobName, @NotNull final InputStream blob);

    /**
     * Sets value (as contents of {@linkplain File}) of blob with specified name. For large blobs (having size larger
     * than {@linkplain PersistentEntityStoreConfig#getMaxInPlaceBlobSize()} which is by default 10000), using this
     * method is preferable, because (in case if the database location and {@code file} exists on a common physical
     * partition) it can succeed by just renaming the file instead of copying.
     *
     * @param blobName name of the blob
     * @param file     blob value as {@linkplain InputStream}
     * @see #getBlob(String)
     * @see #getBlobSize(String)
     * @see #getBlobString(String)
     * @see #setBlob(String, File)
     * @see #setBlobString(String, String)
     * @see #deleteBlob(String)
     * @see PersistentEntityStoreConfig#getMaxInPlaceBlobSize()
     */
    void setBlob(@NotNull final String blobName, @NotNull final File file);

    /**
     * Sets value (as {@linkplain String} encoded in UTF-8) of blob with specified name.
     *
     * @param blobName   name of the blob
     * @param blobString blob value
     * @return {@code true} if blobString is not equal to old value
     * @see #getBlob(String)
     * @see #getBlobSize(String)
     * @see #getBlobString(String)
     * @see jetbrains.exodus.util.UTFUtil#readUTF(InputStream)
     * @see #setBlob(String, InputStream)
     * @see #setBlob(String, File)
     * @see #deleteBlob(String)
     */
    boolean setBlobString(@NotNull final String blobName, @NotNull final String blobString);

    /**
     * Deletes blob with specified name.
     *
     * @param blobName name of the blob
     * @return {@code true} if the blob was actually deleted, otherwise it didn't exist
     * @see #getBlob(String)
     * @see #getBlobSize(String)
     * @see #getBlobString(String)
     * @see #setBlob(String, InputStream)
     * @see #setBlob(String, File)
     * @see #setBlobString(String, String)
     */
    boolean deleteBlob(@NotNull final String blobName);

    /**
     * Returns names of all blobs of the entity.
     *
     * @return {@linkplain List list} of blob names
     * @see #getBlob(String)
     * @see #getBlobSize(String)
     * @see #getBlobString(String)
     * @see #setBlob(String, InputStream)
     * @see #setBlob(String, File)
     * @see #setBlobString(String, String)
     * @see #deleteBlob(String)
     */
    @NotNull
    List<String> getBlobNames();

    /**
     * Adds link with specified name from this entity to specified entity. If there is a link with the same name to
     * another target, it is not affected.
     *
     * @param linkName name of the link
     * @param target   entity to link with
     * @return {@code true} if the link didn't exist
     * @see #getLink(String)
     * @see #getLinks(String)
     * @see #setLink(String, Entity)
     * @see #deleteLink(String, Entity)
     * @see #deleteLinks(String)
     * @see #getLinkNames()
     */
    boolean addLink(@NotNull final String linkName, @NotNull final Entity target);

    /**
     * Returns a single entity which this one is linked by link with specified name, or {@code null}
     * if no such entity exists.
     *
     * @param linkName name of the link
     * @return linked entity or or {@code null} if it doesn't exist
     * @see #addLink(String, Entity)
     * @see #getLinks(String)
     * @see #getLinks(Collection)
     * @see #setLink(String, Entity)
     * @see #deleteLink(String, Entity)
     * @see #deleteLinks(String)
     * @see #getLinkNames()
     */
    @Nullable
    Entity getLink(@NotNull final String linkName);

    /**
     * Links this entity with target one by link with specified name. Unlike {@linkplain #addLink(String, Entity)}, the
     * method doesn't add new target if any with the same link already exists, but overrides the old target. So there
     * can be only 0 or 1 link with specified name set by this method.
     *
     * @param linkName name of the link
     * @param target   entity to link with
     * @return {@code true} if {@code target} is not equal to old target
     * @see #getLink(String)
     * @see #addLink(String, Entity)
     * @see #getLinks(String)
     * @see #getLinks(Collection)
     * @see #deleteLink(String, Entity)
     * @see #deleteLinks(String)
     * @see #getLinkNames()
     */
    boolean setLink(@NotNull final String linkName, @Nullable final Entity target);

    /**
     * Returns all entities which this one is linked by the link with specified name.
     *
     * @param linkName name of the link
     * @return {@linkplain EntityIterable}
     * @see #getLink(String)
     * @see #addLink(String, Entity)
     * @see #setLink(String, Entity)
     * @see #getLinks(Collection)
     * @see #deleteLink(String, Entity)
     * @see #deleteLinks(String)
     * @see #getLinkNames()
     */
    @NotNull
    EntityIterable getLinks(@NotNull final String linkName);

    /**
     * Returns all entities which this one is linked by links with specified names.
     *
     * @param linkNames collection of link names
     * @return {@linkplain EntityIterable}
     * @see #getLink(String)
     * @see #addLink(String, Entity)
     * @see #setLink(String, Entity)
     * @see #getLinks(String)
     * @see #deleteLink(String, Entity)
     * @see #deleteLinks(String)
     * @see #getLinkNames()
     */
    @NotNull
    EntityIterable getLinks(@NotNull final Collection<String> linkNames);

    /**
     * Deletes the link with specified name to specified target.
     *
     * @param linkName name of the link
     * @param target   linked target
     * @return {@code true} is the link was actually deleted, otherwise it didn't exist
     * @see #getLink(String)
     * @see #addLink(String, Entity)
     * @see #setLink(String, Entity)
     * @see #getLinks(String)
     * @see #getLinks(Collection)
     * @see #deleteLinks(String)
     * @see #getLinkNames()
     */
    boolean deleteLink(@NotNull final String linkName, @NotNull final Entity target);

    /**
     * Deletes links to all entities which this entity is linked by the link with specified name.
     *
     * @param linkName name of the link
     * @see #getLink(String)
     * @see #addLink(String, Entity)
     * @see #setLink(String, Entity)
     * @see #getLinks(String)
     * @see #getLinks(Collection)
     * @see #deleteLink(String, Entity)
     * @see #getLinkNames()
     */
    void deleteLinks(@NotNull final String linkName);

    /**
     * Returns names of all links of the entity.
     *
     * @return {@linkplain List list} of link names
     * @see #getLink(String)
     * @see #addLink(String, Entity)
     * @see #setLink(String, Entity)
     * @see #getLinks(String)
     * @see #getLinks(Collection)
     * @see #deleteLink(String, Entity)
     * @see #deleteLinks(String)
     */
    @NotNull
    List<String> getLinkNames();
}
