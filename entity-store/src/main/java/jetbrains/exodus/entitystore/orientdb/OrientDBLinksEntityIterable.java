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

import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Collection;
import java.util.Iterator;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.StoreTransaction;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.functors.EqualPredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class OrientDBLinksEntityIterable implements EntityIterable {

  private final Iterable<OVertex> vertices;

  public OrientDBLinksEntityIterable(Iterable<OVertex> vertices) {
    this.vertices = vertices;
  }

  @Override
  public @NotNull EntityIterator iterator() {
    return new EntityIterator() {
      private final Iterator<OVertex> iterator = vertices.iterator();

      @Override
      public boolean skip(int number) {
        var skipped = 0;
        while (skipped < number && iterator.hasNext()) {
          iterator.next();
          skipped++;
        }

        return skipped == number;
      }

      @Override
      public @Nullable EntityId nextId() {
        if (iterator.hasNext()) {
          var vertex = iterator.next();
          var identity = vertex.getIdentity();

          return new PersistentEntityId(identity.getClusterId(),
              identity.getClusterPosition());
        }

        return null;
      }

      @Override
      public boolean dispose() {
        return true;
      }

      @Override
      public boolean shouldBeDisposed() {
        return false;
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Entity next() {
        return new OrientDBEntity(iterator.next());
      }
    };
  }

  @Override
  public @NotNull StoreTransaction getTransaction() {
    return null;
  }

  @Override
  public boolean isEmpty() {
    return !vertices.iterator().hasNext();
  }

  @Override
  public long size() {
    if (vertices instanceof Collection) {
      return ((Collection<?>) vertices).size();
    } else if (vertices instanceof OSizeable) {
      return ((OSizeable) vertices).size();
    } else {
      return IterableUtils.size(vertices);
    }
  }

  @Override
  public long count() {
    if (vertices instanceof Collection) {
      return ((Collection<?>) vertices).size();
    } else if (vertices instanceof OSizeable) {
      return ((OSizeable) vertices).size();
    } else {
      return -1;
    }
  }

  @Override
  public long getRoughCount() {
    return count();
  }

  @Override
  public long getRoughSize() {
    return count();
  }

  @Override
  public int indexOf(@NotNull Entity entity) {
    return IteratorUtils.indexOf(iterator(), EqualPredicate.equalPredicate(entity));
  }

  @Override
  public boolean contains(@NotNull Entity entity) {
    return IteratorUtils.contains(iterator(), entity);
  }

  @Override
  public @NotNull EntityIterable intersect(@NotNull EntityIterable right) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable intersectSavingOrder(@NotNull EntityIterable right) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable union(@NotNull EntityIterable right) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable minus(@NotNull EntityIterable right) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable concat(@NotNull EntityIterable right) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable skip(int number) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable take(int number) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable distinct() {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable selectDistinct(@NotNull String linkName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull EntityIterable selectManyDistinct(@NotNull String linkName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @Nullable Entity getFirst() {
    var iterator = iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    }

    return null;
  }

  @Override
  public @Nullable Entity getLast() {
    var iterator = iterator();

    Entity last = null;
    if (iterator.hasNext()) {
      last = iterator.next();
    }

    return last;
  }

  @Override
  public @NotNull EntityIterable reverse() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSortResult() {
    return false;
  }

  @Override
  public @NotNull EntityIterable asSortResult() {
    return this;
  }
}
