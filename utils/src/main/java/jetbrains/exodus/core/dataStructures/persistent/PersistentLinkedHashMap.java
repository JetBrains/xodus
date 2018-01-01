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
package jetbrains.exodus.core.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.core.dataStructures.hash.PairProcedure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class PersistentLinkedHashMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(PersistentLinkedHashMap.class);

    @Nullable
    private volatile Root<K, V> root;
    @Nullable
    private final RemoveEldestFunction<K, V> removeEldest;

    public PersistentLinkedHashMap() {
        root = null;
        removeEldest = null;
    }

    public PersistentLinkedHashMap(@Nullable final RemoveEldestFunction<K, V> removeEldest) {
        root = null;
        this.removeEldest = removeEldest;
    }

    private PersistentLinkedHashMap(@NotNull final PersistentLinkedHashMap<K, V> source, @Nullable final RemoveEldestFunction<K, V> removeEldest) {
        final Root<K, V> sourceRoot = source.root;
        if (sourceRoot == null) {
            root = new Root<>();
        } else {
            root = new Root<>(sourceRoot);
        }
        this.removeEldest = removeEldest;
    }

    public int size() {
        final Root<K, V> root = this.root;
        return root == null ? 0 : root.map.getCurrent().size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public PersistentLinkedHashMap<K, V> getClone() {
        return new PersistentLinkedHashMap<>(this, removeEldest);
    }

    public PersistentLinkedHashMap<K, V> getClone(@Nullable final RemoveEldestFunction<K, V> removeEldestFunction) {
        return new PersistentLinkedHashMap<>(this, removeEldestFunction);
    }

    public PersistentLinkedHashMapMutable<K, V> beginWrite() {
        return new PersistentLinkedHashMapMutable<>(this);
    }

    public boolean endWrite(@NotNull final PersistentLinkedHashMapMutable<K, V> mutableMap) {
        if (!mutableMap.isDirty() || (root != null && mutableMap.root != root)) {
            return false;
        }
        // TODO: this is a relaxed condition (not to break existing behaviour)
        boolean result = mutableMap.endWrite();
        root = new Root<>(mutableMap.root.map, mutableMap.root.queue, mutableMap.order);
        return result;
    }

    public static class PersistentLinkedHashMapMutable<K, V> implements Iterable<Pair<K, V>> {

        @NotNull
        private final Root<K, V> root;
        private long order;
        @Nullable
        private final RemoveEldestFunction<K, V> removeEldest;
        @NotNull
        private final PersistentHashMap<K, InternalValue<V>>.MutablePersistentHashMap mapMutable;
        @NotNull
        private final PersistentLongMap.MutableMap<K> queueMutable;
        private boolean isDirty;

        public PersistentLinkedHashMapMutable(@NotNull final PersistentLinkedHashMap<K, V> source) {
            final Root<K, V> sourceRoot = source.root;
            if (sourceRoot == null) {
                root = new Root<>();
                order = 0L;
            } else {
                root = sourceRoot;
                order = sourceRoot.order;
            }
            removeEldest = source.removeEldest;
            mapMutable = root.map.beginWrite();
            queueMutable = root.queue.beginWrite();
            isDirty = false;
        }

        @Nullable
        public V get(@NotNull final K key) {
            final InternalValue<V> internalValue = mapMutable.get(key);
            if (internalValue == null) {
                return null;
            }
            final V result = internalValue.getValue();
            final long currentOrder = internalValue.getOrder();
            if (root.order > currentOrder + (mapMutable.size() >> 1)) {
                isDirty = true;
                final long newOrder = ++order;
                mapMutable.put(key, new InternalValue<>(newOrder, result));
                queueMutable.put(newOrder, key);
                removeKeyAndCheckConsistency(key, currentOrder);
            }
            return result;
        }

        @Nullable
        public V getValue(@NotNull final K key) {
            final InternalValue<V> internalValue = mapMutable.get(key);
            return internalValue == null ? null : internalValue.getValue();
        }

        public boolean containsKey(@NotNull final K key) {
            return mapMutable.get(key) != null;
        }

        public void put(@NotNull final K key, @Nullable final V value) {
            final InternalValue<V> internalValue = mapMutable.get(key);
            if (internalValue != null) {
                removeKeyAndCheckConsistency(key, internalValue.getOrder());
            }
            isDirty = true;
            final long newOrder = ++order;
            mapMutable.put(key, new InternalValue<>(newOrder, value));
            queueMutable.put(newOrder, key);
            if (removeEldest != null) {
                int removed = 0;
                PersistentLongMap.Entry<K> min;
                while ((min = queueMutable.getMinimum()) != null) {
                    if (removed >= 50) {
                        break; // prevent looping on implementation errors
                    }
                    final K eldestKey = min.getValue();
                    if (removeEldest.removeEldest(this, eldestKey, getValue(eldestKey))) {
                        isDirty = true;
                        mapMutable.removeKey(eldestKey);   // removeKey may do nothing, but we still must
                        queueMutable.remove(min.getKey()); // remove min key from the order queue
                        removed++;
                    } else {
                        break;
                    }
                }
                if (removed >= 35 && logger.isWarnEnabled()) {
                    logger.warn("PersistentLinkedHashMap evicted " + removed + " keys during a single put().", new Throwable());
                }
            }
        }

        public V remove(@NotNull final K key) {
            final InternalValue<V> internalValue = mapMutable.removeKey(key);
            if (internalValue != null) {
                isDirty = true;
                removeKeyAndCheckConsistency(key, internalValue.getOrder());
                return internalValue.getValue();
            }
            return null;
        }

        public int size() {
            return mapMutable.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void forEachKey(final ObjectProcedure<K> procedure) {
            mapMutable.forEachKey(new ObjectProcedure<PersistentHashMap.Entry<K, InternalValue<V>>>() {
                @Override
                public boolean execute(PersistentHashMap.Entry<K, InternalValue<V>> object) {
                    return procedure.execute(object.getKey());
                }
            });
        }

        public void forEachEntry(final PairProcedure<K, V> procedure) {
            mapMutable.forEachKey(new ObjectProcedure<PersistentHashMap.Entry<K, InternalValue<V>>>() {
                @Override
                public boolean execute(PersistentHashMap.Entry<K, InternalValue<V>> object) {
                    return procedure.execute(object.getKey(), object.getValue().getValue());
                }
            });
        }

        public boolean isDirty() {
            return isDirty;
        }

        @Override
        public Iterator<Pair<K, V>> iterator() {
            final Iterator<PersistentHashMap.Entry<K, InternalValue<V>>> sourceIt = mapMutable.iterator();
            return new Iterator<Pair<K, V>>() {

                @Override
                public boolean hasNext() {
                    return sourceIt.hasNext();
                }

                @Override
                public Pair<K, V> next() {
                    final PersistentHashMap.Entry<K, InternalValue<V>> next = sourceIt.next();
                    return new Pair<>(next.getKey(), next.getValue().getValue());
                }

                @Override
                public void remove() {
                    sourceIt.remove();
                }
            };
        }

        boolean endWrite() {
            return mapMutable.endWrite() && queueMutable.endWrite();
        }

        void checkTip() {
            mapMutable.checkTip();
            queueMutable.testConsistency();
        }

        private void removeKeyAndCheckConsistency(@NotNull final K key, final long prevOrder) {
            final K keyByOrder = queueMutable.remove(prevOrder);
            if (!key.equals(keyByOrder)) {
                logger.error("PersistentLinkedHashMap is inconsistent, key = " + key + ", keyByOrder = " + keyByOrder +
                        ", prevOrder = " + prevOrder, new Throwable());
            }
        }
    }

    /**
     * Logs the error that the map is inconsistent instead of throwing an exception. This leaves the chance for
     * the map to recovery to a consistent state.
     */
    static void logMapIsInconsistent() {
        logger.error("PersistentLinkedHashMap is inconsistent", new Throwable());
    }

    public interface RemoveEldestFunction<K, V> {

        boolean removeEldest(@NotNull final PersistentLinkedHashMapMutable<K, V> map,
                             @NotNull final K key,
                             @Nullable final V value);
    }

    private static class Root<K, V> {

        @NotNull
        private final PersistentHashMap<K, InternalValue<V>> map;
        @NotNull
        private final PersistentLongMap<K> queue;
        private final long order;

        private Root() {
            this(new PersistentHashMap<K, InternalValue<V>>(), new PersistentLong23TreeMap<K>(), 0L);
        }

        private Root(@NotNull final Root<K, V> source) {
            this(source.map.getClone(), source.queue.getClone(), source.order);
        }

        private Root(@NotNull final PersistentHashMap<K, InternalValue<V>> map,
                     @NotNull PersistentLongMap<K> queue, final long order) {
            this.map = map;
            this.queue = queue;
            this.order = order;
        }
    }

    private static class InternalValue<V> {

        private final long order;
        private final V value;

        InternalValue(long order, V value) {
            this.order = order;
            this.value = value;
        }

        public long getOrder() {
            return order;
        }

        public V getValue() {
            return value;
        }
    }
}
