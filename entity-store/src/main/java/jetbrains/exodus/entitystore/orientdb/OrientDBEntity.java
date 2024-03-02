/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.EntityStore;
import jetbrains.exodus.entitystore.PersistentEntityId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class OrientDBEntity implements Entity {

  private final OVertex vertex;
  private long txid;

  public OrientDBEntity(OVertex vertex) {
    this.vertex = vertex;

    var session = ODatabaseSession.getActiveSession();
    var tx = session.getTransaction();
    txid = tx.getId();
  }

  @Override
  public @NotNull EntityStore getStore() {
    return null;
  }

  @Override
  public @NotNull EntityId getId() {
    var id = vertex.getIdentity();
    return new PersistentEntityId(id.getClusterId(), id.getClusterPosition());
  }

  @Override
  public @NotNull String toIdString() {
    return vertex.getIdentity().toString();
  }

  @Override
  public @NotNull String getType() {
    return vertex.getSchemaClass().getName();
  }

  @Override
  public boolean delete() {
    vertex.delete();
    return false;
  }

  @Override
  public @Nullable ByteIterable getRawProperty(@NotNull String propertyName) {
    return null;
  }

  @Override
  public @Nullable Comparable getProperty(@NotNull String propertyName) {
    reload();
    return getProperty(propertyName);
  }

  private void reload() {
    var session = ODatabaseSession.getActiveSession();
    var tx = session.getTransaction();

    if (txid != tx.getId()) {
      txid = tx.getId();
      vertex.reload();
    }
  }

  @Override
  public boolean setProperty(@NotNull String propertyName, @NotNull Comparable value) {
    reload();
    vertex.setProperty(propertyName, value);
    return false;
  }

  @Override
  public boolean deleteProperty(@NotNull String propertyName) {
    return false;
  }

  @Override
  public @NotNull List<String> getPropertyNames() {
    reload();
    return new ArrayList<>(vertex.getPropertyNames());
  }

  @Override
  public @Nullable InputStream getBlob(@NotNull String blobName) {
    return null;
  }

  @Override
  public long getBlobSize(@NotNull String blobName) {
    return 0;
  }

  @Override
  public @Nullable String getBlobString(@NotNull String blobName) {
    return null;
  }

  @Override
  public void setBlob(@NotNull String blobName, @NotNull InputStream blob) {

  }

  @Override
  public void setBlob(@NotNull String blobName, @NotNull File file) {

  }

  @Override
  public boolean setBlobString(@NotNull String blobName, @NotNull String blobString) {
    return false;
  }

  @Override
  public boolean deleteBlob(@NotNull String blobName) {
    return false;
  }

  @Override
  public @NotNull List<String> getBlobNames() {
    return null;
  }

  @Override
  public boolean addLink(@NotNull String linkName, @NotNull Entity target) {
    return false;
  }

  @Override
  public boolean addLink(@NotNull String linkName, @NotNull EntityId targetId) {
    return false;
  }

  @Override
  public @Nullable Entity getLink(@NotNull String linkName) {
    return null;
  }

  @Override
  public boolean setLink(@NotNull String linkName, @Nullable Entity target) {
    return false;
  }

  @Override
  public boolean setLink(@NotNull String linkName, @NotNull EntityId targetId) {
    return false;
  }

  @Override
  public @NotNull EntityIterable getLinks(@NotNull String linkName) {
    reload();
    return new OrientDBLinksEntityIterable(vertex.getVertices(ODirection.OUT, linkName));
  }

  @Override
  public @NotNull EntityIterable getLinks(@NotNull Collection<String> linkNames) {
    reload();
    return new OrientDBLinksEntityIterable(
        vertex.getVertices(ODirection.OUT, linkNames.toArray(new String[0])));
  }

  @Override
  public boolean deleteLink(@NotNull String linkName, @NotNull Entity target) {
    return false;
  }

  @Override
  public boolean deleteLink(@NotNull String linkName, @NotNull EntityId targetId) {
    return false;
  }

  @Override
  public void deleteLinks(@NotNull String linkName) {

  }

  @Override
  public @NotNull List<String> getLinkNames() {
    reload();
    return new ArrayList<>(vertex.getEdgeNames(ODirection.OUT));
  }

  @Override
  public int compareTo(@NotNull Entity o) {
    return 0;
  }
}
