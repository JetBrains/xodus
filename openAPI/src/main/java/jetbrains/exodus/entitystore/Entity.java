/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

public interface Entity extends Comparable<Entity> {

    @NotNull
    EntityStore getStore();

    @NotNull
    EntityId getId();

    @NotNull
    String toIdString();

    @NotNull
    String getType();

    int getVersion();

    boolean isUpToDate();

    /**
     * Returns old versions of the entity starting from the newest one.
     *
     * @return history items.
     */
    @Deprecated
    @NotNull
    List<Entity> getHistory();

    @Deprecated
    @Nullable
    Entity getNextVersion();

    @Deprecated
    @Nullable
    Entity getPreviousVersion();

    /**
     * Deletes the entity unconditionally.
     *
     * @return true if entity existed
     */
    boolean delete();

    @Nullable
    ByteIterable getRawProperty(@NotNull final String propertyName);

    /**
     * Returns the value of specified property of this entity, or null if it is not set.
     *
     * @param propertyName name of the property.
     * @return property value.
     */
    @Nullable
    Comparable getProperty(@NotNull final String propertyName);

    /**
     * Sets specified property of this entity to specified value. The value is indexed, i.e.
     * the entity will be returned amongst results of find() operation by the property value.
     *
     * @param propertyName name of the property.
     * @param value        property value.
     * @return true if value is not equal to old value
     */
    boolean setProperty(@NotNull final String propertyName, @NotNull final Comparable value);

    /**
     * Deletes specified property of this entity.
     *
     * @param propertyName name of the property.
     * @return true if property existed
     */
    boolean deleteProperty(@NotNull final String propertyName);

    /**
     * Returns names of all properties of the entity.
     *
     * @return property names.
     */
    @NotNull
    List<String> getPropertyNames();

    @Nullable
    InputStream getBlob(@NotNull final String blobName);

    long getBlobSize(@NotNull final String blobName);

    @Nullable
    String getBlobString(@NotNull final String blobName);

    void setBlob(@NotNull final String blobName, @NotNull final InputStream blob);

    void setBlob(@NotNull final String blobName, @NotNull final File file);

    /**
     * @param blobName
     * @param blobString
     * @return true if blobString is not equal to old value
     */
    boolean setBlobString(@NotNull final String blobName, @NotNull final String blobString);

    /**
     * @param blobName
     * @return true if property existed
     */
    boolean deleteBlob(@NotNull final String blobName);

    @NotNull
    List<String> getBlobNames();

    /**
     * Adds specified link from this entity to specified entity.
     *
     * @param linkName name of the link.
     * @param target   entity to link with.
     * @return true if link was not existed
     */
    boolean addLink(@NotNull final String linkName, @NotNull final Entity target);

    /**
     * Returns a single entity which this one is linked with specified link, or null
     * if no such entity exists.
     *
     * @param linkName name of urged link
     * @return linked entity
     */
    @Nullable
    Entity getLink(@NotNull final String linkName);

    /**
     * @param linkName
     * @param target
     * @return true if target is not equal to old target
     */
    boolean setLink(@NotNull final String linkName, @Nullable final Entity target);

    /**
     * Returns all entities which this one is linked with specified link.
     *
     * @param linkName name of the link.
     * @return Iterable<Entity> object.
     */
    @NotNull
    EntityIterable getLinks(@NotNull final String linkName);

    @NotNull
    EntityIterable getLinks(@NotNull final Collection<String> linkNames);

    /**
     * Deletes a link to an entity.
     *
     * @param linkName name of the link.
     * @param entity   linked entity.
     */
    boolean deleteLink(@NotNull final String linkName, @NotNull final Entity entity);

    /**
     * Deletes links to all entities which this entity linked with a link.
     *
     * @param linkName name of the link.
     */
    void deleteLinks(@NotNull final String linkName);

    /**
     * Returns names of all links of the entity.
     *
     * @return link names.
     */
    @NotNull
    List<String> getLinkNames();

}
