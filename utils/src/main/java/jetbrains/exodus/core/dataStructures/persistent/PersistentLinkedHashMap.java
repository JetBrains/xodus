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
package jetbrains.exodus.core.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.core.dataStructures.hash.PairProcedure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentLinkedHashMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(PersistentLinkedHashMap.class);

    @Nullable
    private volatile Pair<PersistentHashMap<K, InternalValue<V>>, PersistentLong23TreeMap<K>> root;
    @NotNull
    private final AtomicLong orderCounter;
    @Nullable
    private final RemoveEldestFunction<K, V> removeEldest;

    public PersistentLinkedHashMap() {
        root = null;
        orderCounter = new AtomicLong();
        removeEldest = null;
    }

    public PersistentLinkedHashMap(@Nullable final RemoveEldestFunction<K, V> removeEldest) {
        root = null;
        orderCounter = new AtomicLong();
        this.removeEldest = removeEldest;
    }

    private PersistentLinkedHashMap(@NotNull final PersistentLinkedHashMap<K, V> source, @Nullable final RemoveEldestFunction<K, V> removeEldest) {
        final Pair<PersistentHashMap<K, InternalValue<V>>, PersistentLong23TreeMap<K>> sourceRoot = source.root;
        if (sourceRoot == null) {
            root = new Pair<>(
                    new PersistentHashMap<K, InternalValue<V>>(), new PersistentLong23TreeMap<K>());
        } else {
            root = new Pair<>(
                    sourceRoot.getFirst().getClone(), sourceRoot.getSecond().getClone());
        }
        orderCounter = source.orderCounter;
        this.removeEldest = removeEldest;
    }

    public int size() {
        final Pair<PersistentHashMap<K, InternalValue<V>>, PersistentLong23TreeMap<K>> root = this.root;
        return root == null ? 0 : root.getFirst().getCurrent().size();
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
        if (!mutableMap.isDirty() || mutableMap.getSourceRoot() != root) {
            return false;
        }
        mutableMap.getMapMutable().endWrite();
        mutableMap.getQueueMutable().endWrite();
        root = new Pair<>(
                mutableMap.getMap(), mutableMap.getQueue());
        return true;
    }

    public static class PersistentLinkedHashMapMutable<K, V> implements Iterable<Pair<K, V>> {

        @Nullable
        private final Pair<PersistentHashMap<K, InternalValue<V>>, PersistentLong23TreeMap<K>> sourceRoot;
        @NotNull
        private final AtomicLong orderCounter;
        @Nullable
        private final RemoveEldestFunction<K, V> removeEldest;
        @NotNull
        private final PersistentHashMap<K, InternalValue<V>> map;
        @NotNull
        private final PersistentHashMap<K, InternalValue<V>>.MutablePersistentHashMap mapMutable;
        @NotNull
        private final PersistentLong23TreeMap<K> queue;
        @NotNull
        private final PersistentLong23TreeMap<K>.MutableMap queueMutable;
        private boolean isDirty;

        public PersistentLinkedHashMapMutable(PersistentLinkedHashMap<K, V> source) {
            sourceRoot = source.root;
            orderCounter = source.orderCounter;
            removeEldest = source.removeEldest;
            if (sourceRoot == null) {
                map = new PersistentHashMap<>();
                queue = new PersistentLong23TreeMap<>();
            } else {
                map = sourceRoot.getFirst();
                queue = sourceRoot.getSecond();
            }
            mapMutable = map.beginWrite();
            queueMutable = queue.beginWrite();
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
            if (orderCounter.get() > currentOrder + (mapMutable.size() >> 1)) {
                isDirty = true;
                final long newOrder = orderCounter.incrementAndGet();
                mapMutable.put(key, new InternalValue<>(newOrder, result));
                queueMutable.put(newOrder, key);
                if (!key.equals(queueMutable.remove(currentOrder))) {
                    logMapIsInconsistent();
                }
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
                if (!key.equals(queueMutable.remove(internalValue.getOrder()))) {
                    logMapIsInconsistent();
                }
            }
            isDirty = true;
            final long newOrder = orderCounter.incrementAndGet();
            mapMutable.put(key, new InternalValue<>(newOrder, value));
            queueMutable.put(newOrder, key);
            if (removeEldest != null) {
                final PersistentLong23TreeMap.Entry<K> min = queueMutable.getMinimum();
                if (min != null) {
                    final K eldestKey = min.getValue();
                    if (removeEldest.removeEldest(this, eldestKey, getValue(eldestKey))) {
                        remove(eldestKey);
                    }
                }
            }
        }

        public V remove(@NotNull final K key) {
            final InternalValue<V> internalValue = mapMutable.removeKey(key);
            if (internalValue != null) {
                isDirty = true;
                if (!key.equals(queueMutable.remove(internalValue.getOrder()))) {
                    logMapIsInconsistent();
                }
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

        @Nullable
        public Pair<PersistentHashMap<K, InternalValue<V>>, PersistentLong23TreeMap<K>> getSourceRoot() {
            return sourceRoot;
        }

        @NotNull
        public PersistentHashMap<K, InternalValue<V>> getMap() {
            return map;
        }

        @NotNull
        public PersistentHashMap<K, InternalValue<V>>.MutablePersistentHashMap getMapMutable() {
            return mapMutable;
        }

        @NotNull
        public PersistentLong23TreeMap<K> getQueue() {
            return queue;
        }

        @NotNull
        public PersistentLong23TreeMap<K>.MutableMap getQueueMutable() {
            return queueMutable;
        }

        void checkTip() {
            mapMutable.checkTip();
            queueMutable.checkTip();
        }
    }

    /**
     * Logs the error that the map is inconsistent instead of throwing an exception. This leaves the chance for
     * the map to recovery to a consistent state.
     */
    private static void logMapIsInconsistent() {
        logger.error("PersistentLinkedHashMap is inconsistent", new Throwable());
    }

    public interface RemoveEldestFunction<K, V> {

        boolean removeEldest(@NotNull final PersistentLinkedHashMapMutable<K, V> map,
                             @NotNull final K key,
                             @Nullable final V value);
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
