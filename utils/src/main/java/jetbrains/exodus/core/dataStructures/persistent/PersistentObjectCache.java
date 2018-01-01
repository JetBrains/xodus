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

import jetbrains.exodus.core.dataStructures.CacheHitRateable;
import jetbrains.exodus.core.dataStructures.ObjectCacheBase;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.core.dataStructures.hash.PairProcedure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static jetbrains.exodus.core.dataStructures.persistent.PersistentLinkedHashMap.logMapIsInconsistent;

public class PersistentObjectCache<K, V> extends CacheHitRateable {

    private final int size;
    private final int firstGenSizeBound;
    private final int secondGenSizeBound;
    private final AtomicReference<Root<K, V>> root;

    public PersistentObjectCache() {
        this(ObjectCacheBase.DEFAULT_SIZE);
    }

    public PersistentObjectCache(int size) {
        this(size, 0.5f);
    }

    public PersistentObjectCache(final int size, float secondGenSizeRatio) {
        this.size = size < ObjectCacheBase.MIN_SIZE ? ObjectCacheBase.MIN_SIZE : size;
        if (secondGenSizeRatio < 0.05f) {
            secondGenSizeRatio = 0.05f;
        } else if (secondGenSizeRatio > 0.95f) {
            secondGenSizeRatio = 0.95f;
        }
        secondGenSizeBound = (int) (size * secondGenSizeRatio);
        firstGenSizeBound = size - secondGenSizeBound;
        root = new AtomicReference<>();
    }

    protected PersistentObjectCache(@NotNull final PersistentObjectCache<K, V> source, @Nullable final EvictListener<K, V> listener) {
        size = source.size;
        firstGenSizeBound = source.firstGenSizeBound;
        secondGenSizeBound = source.secondGenSizeBound;
        root = new AtomicReference<>(Root.getClone(source.root.get(), listener, firstGenSizeBound, secondGenSizeBound));
        setAttempts(source.getAttempts());
        setHits(source.getHits());
    }

    public void clear() {
        root.set(null);
    }

    public int size() {
        return size;
    }

    public int count() {
        final Root<K, V> root = getCurrent();
        return root == null ? 0 : root.getFirstGen().size() + root.getSecondGen().size();
    }

    public V get(@NotNull final K key) {
        return tryKey(key);
    }

    public void put(@NotNull final K key, @NotNull final V x) {
        cacheObject(key, x);
    }

    public V tryKey(@NotNull final K key) {
        incAttempts();
        Root<K, V> current;
        Root<K, V> next;
        V result;
        do {
            current = getCurrent();
            next = new Root<>(current, firstGenSizeBound, secondGenSizeBound);
            final PersistentLinkedHashMap<K, V> secondGen = next.getSecondGen();
            final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> secondGenMutable = secondGen.beginWrite();
            result = secondGenMutable.get(key);
            boolean wereMutations = secondGenMutable.isDirty();
            if (result == null) {
                final PersistentLinkedHashMap<K, V> firstGen = next.getFirstGen();
                final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> firstGenMutable = firstGen.beginWrite();
                result = firstGenMutable.get(key);
                if (!firstGenMutable.isDirty() && firstGenMutable.size() >= (firstGenSizeBound >> 1)) {
                    // if result is not null then move it to the second generation
                    if (result != null) {
                        firstGenMutable.remove(key);
                        secondGenMutable.put(key, result);
                    }
                }
                if (firstGenMutable.isDirty()) {
                    wereMutations = true;
                    if (!firstGen.endWrite(firstGenMutable)) {
                        logMapIsInconsistent();
                    }
                }
            }
            if (!wereMutations) {
                break;
            }
            if (secondGenMutable.isDirty() && !secondGen.endWrite(secondGenMutable)) {
                logMapIsInconsistent();
            }
        } while (!root.compareAndSet(current, next));
        if (result != null) {
            incHits();
        }
        return result;
    }

    public V getObject(@NotNull final K key) {
        final Root<K, V> current = getCurrent();
        if (current == null) {
            return null;
        }
        V result = current.getFirstGen().beginWrite().getValue(key);
        if (result == null) {
            result = current.getSecondGen().beginWrite().getValue(key);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public void cacheObject(@NotNull final K key, @NotNull final V x) {
        Root<K, V> current;
        Root<K, V> next;
        do {
            current = getCurrent();
            next = new Root<>(current, firstGenSizeBound, secondGenSizeBound);
            final PersistentLinkedHashMap<K, V> firstGen = next.getFirstGen();
            final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> firstGenMutable = firstGen.beginWrite();
            final PersistentLinkedHashMap<K, V> secondGen = next.getSecondGen();
            final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> secondGenMutable = secondGen.beginWrite();
            if (firstGenMutable.remove(key) == null) {
                secondGenMutable.remove(key);
            }
            if (secondGenMutable.size() < (secondGenSizeBound >> 1)) {
                secondGenMutable.put(key, x);
            } else {
                firstGenMutable.put(key, x);
            }
            if (firstGenMutable.isDirty() && !firstGen.endWrite(firstGenMutable)) {
                logMapIsInconsistent();
            }
            if (secondGenMutable.isDirty() && !secondGen.endWrite(secondGenMutable)) {
                logMapIsInconsistent();
            }
        } while (!root.compareAndSet(current, next));
    }

    public V remove(@NotNull final K key) {
        Root<K, V> current;
        Root<K, V> next;
        V result;
        do {
            current = getCurrent();
            next = new Root<>(current, firstGenSizeBound, secondGenSizeBound);
            final PersistentLinkedHashMap<K, V> firstGen = next.getFirstGen();
            final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> firstGenMutable = firstGen.beginWrite();
            result = firstGenMutable.remove(key);
            if (result != null) {
                if (!firstGen.endWrite(firstGenMutable)) {
                    logMapIsInconsistent();
                }
            } else {
                final PersistentLinkedHashMap<K, V> secondGen = next.getSecondGen();
                final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> secondGenMutable = secondGen.beginWrite();
                result = secondGenMutable.remove(key);
                if (result == null) {
                    break;
                }
                if (!secondGen.endWrite(secondGenMutable)) {
                    logMapIsInconsistent();
                }
            }
        } while (!root.compareAndSet(current, next));
        return result;
    }

    public void forEachKey(final ObjectProcedure<K> procedure) {
        final Root<K, V> current = getCurrent();
        if (current == null) {
            return;
        }
        current.getFirstGen().beginWrite().forEachKey(procedure);
        current.getSecondGen().beginWrite().forEachKey(procedure);
    }

    public void forEachEntry(final PairProcedure<K, V> procedure) {
        final Root<K, V> current = getCurrent();
        if (current == null) {
            return;
        }
        current.getFirstGen().beginWrite().forEachEntry(procedure);
        current.getSecondGen().beginWrite().forEachEntry(procedure);
    }

    public Iterator<K> keys() {
        final Root<K, V> current = getCurrent();
        if (current == null) {
            return new ArrayList<K>(1).iterator();
        }
        return new Iterator<K>() {

            private Iterator<Pair<K, V>> firstGenIt = current.getFirstGen().beginWrite().iterator();
            private Iterator<Pair<K, V>> secondGenIt = null;
            private K next = null;

            @Override
            public boolean hasNext() {
                checkNext();
                return next != null;
            }

            @Override
            public K next() {
                checkNext();
                final K result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("PersistentObjectCache.keys iterator is immutable");
            }

            private void checkNext() {
                if (next == null) {
                    if (firstGenIt != null) {
                        if (firstGenIt.hasNext()) {
                            next = firstGenIt.next().getFirst();
                            return;
                        }
                        firstGenIt = null;
                        secondGenIt = current.getSecondGen().beginWrite().iterator();
                    }
                    if (secondGenIt != null) {
                        if (secondGenIt.hasNext()) {
                            next = secondGenIt.next().getFirst();
                            return;
                        }
                        secondGenIt = null;
                    }
                }
            }
        };
    }

    public Iterator<V> values() {
        final Root<K, V> current = getCurrent();
        if (current == null) {
            return new ArrayList<V>(1).iterator();
        }
        final ArrayList<V> result = new ArrayList<>();
        for (final Pair<K, V> pair : current.getFirstGen().beginWrite()) {
            result.add(pair.getSecond());
        }
        for (final Pair<K, V> pair : current.getSecondGen().beginWrite()) {
            result.add(pair.getSecond());
        }
        return result.iterator();
    }

    public PersistentObjectCache<K, V> getClone(@Nullable final EvictListener<K, V> listener) {
        return new PersistentObjectCache<>(this, listener);
    }

    private Root<K, V> getCurrent() {
        return root.get();
    }

    private static class Root<K, V> {

        @NotNull
        private final PersistentLinkedHashMap<K, V> firstGen;
        @NotNull
        private final PersistentLinkedHashMap<K, V> secondGen;

        private Root(@Nullable final Root<K, V> sourceRoot, final int firstGenSizeBound, final int secondGenSizeBound) {
            if (sourceRoot != null) {
                firstGen = sourceRoot.firstGen.getClone();
                secondGen = sourceRoot.secondGen.getClone();
            } else {
                firstGen = new PersistentLinkedHashMap<>(new PersistentLinkedHashMap.RemoveEldestFunction<K, V>() {
                    @Override
                    public boolean removeEldest(@NotNull final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> map,
                                                @NotNull final K key, @Nullable final V value) {
                        return map.size() > firstGenSizeBound;
                    }
                });
                secondGen = new PersistentLinkedHashMap<>(new PersistentLinkedHashMap.RemoveEldestFunction<K, V>() {
                    @Override
                    public boolean removeEldest(@NotNull final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> map,
                                                @NotNull final K key, @Nullable final V value) {
                        return map.size() > secondGenSizeBound;
                    }
                });
            }
        }

        public Root(@NotNull PersistentLinkedHashMap<K, V> firstGen, @NotNull PersistentLinkedHashMap<K, V> secondGen) {
            this.firstGen = firstGen;
            this.secondGen = secondGen;
        }

        @NotNull
        public PersistentLinkedHashMap<K, V> getFirstGen() {
            return firstGen;
        }

        @NotNull
        public PersistentLinkedHashMap<K, V> getSecondGen() {
            return secondGen;
        }

        static <K, V> Root<K, V> getClone(@Nullable final Root<K, V> sourceRoot,
                                          @Nullable final EvictListener<K, V> listener,
                                          final int firstGenSizeBound, final int secondGenSizeBound) {
            if (listener == null) {
                final PersistentLinkedHashMap.RemoveEldestFunction<K, V> firstGenEvict = new PersistentLinkedHashMap.RemoveEldestFunction<K, V>() {
                    @Override
                    public boolean removeEldest(@NotNull final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> map,
                                                @NotNull final K key, @Nullable final V value) {
                        return map.size() > firstGenSizeBound;
                    }
                };
                final PersistentLinkedHashMap.RemoveEldestFunction<K, V> secondGenEvict = new PersistentLinkedHashMap.RemoveEldestFunction<K, V>() {
                    @Override
                    public boolean removeEldest(@NotNull final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> map,
                                                @NotNull final K key, @Nullable final V value) {
                        return map.size() > secondGenSizeBound;
                    }
                };
                if (sourceRoot != null) {
                    return new Root<>(sourceRoot.firstGen.getClone(firstGenEvict), sourceRoot.secondGen.getClone(secondGenEvict));
                } else {
                    return new Root<>(new PersistentLinkedHashMap<>(firstGenEvict), new PersistentLinkedHashMap<>(secondGenEvict));
                }
            }

            final PersistentLinkedHashMap.RemoveEldestFunction<K, V> firstGenEvict = new PersistentLinkedHashMap.RemoveEldestFunction<K, V>() {
                @Override
                public boolean removeEldest(@NotNull final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> map,
                                            @NotNull final K key, @Nullable final V value) {
                    if (map.size() > firstGenSizeBound) {
                        listener.onEvict(key, value);
                        return true;
                    }
                    return false;
                }
            };
            final PersistentLinkedHashMap.RemoveEldestFunction<K, V> secondGenEvict = new PersistentLinkedHashMap.RemoveEldestFunction<K, V>() {
                @Override
                public boolean removeEldest(@NotNull final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<K, V> map,
                                            @NotNull final K key, @Nullable final V value) {
                    if (map.size() > secondGenSizeBound) {
                        listener.onEvict(key, value);
                        return true;
                    }
                    return false;
                }
            };
            if (sourceRoot != null) {
                return new Root<>(sourceRoot.firstGen.getClone(firstGenEvict), sourceRoot.secondGen.getClone(secondGenEvict));
            } else {
                return new Root<>(new PersistentLinkedHashMap<>(firstGenEvict), new PersistentLinkedHashMap<>(secondGenEvict));
            }
        }
    }
}
