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
package jetbrains.exodus.core.dataStructures;

import jetbrains.exodus.core.dataStructures.hash.LongLinkedHashMap;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import org.jetbrains.annotations.NotNull;

import java.util.EventListener;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LongObjectCache<V> extends LongObjectCacheBase<V> {

    public static final float DEFAULT_SECOND_GENERATION_QUEUE_SIZE_RATIO = 0.4f;

    private final Lock lock;
    private final float secondGenSizeRatio;
    private LongLinkedHashMap<V> firstGenerationQueue;
    private LongLinkedHashMap<V> secondGenerationQueue;
    private DeletedPairsListener<V>[] listeners;
    private V pushedOutValue;

    public LongObjectCache() {
        this(DEFAULT_SIZE);
    }

    public LongObjectCache(int cacheSize) {
        this(cacheSize, DEFAULT_SECOND_GENERATION_QUEUE_SIZE_RATIO);
    }

    @SuppressWarnings({"unchecked"})
    public LongObjectCache(int cacheSize, float secondGenSizeRatio) {
        super(cacheSize);
        lock = new ReentrantLock();
        if (secondGenSizeRatio < 0.05f) {
            secondGenSizeRatio = 0.05f;
        } else if (secondGenSizeRatio > 0.95f) {
            secondGenSizeRatio = 0.95f;
        }
        this.secondGenSizeRatio = secondGenSizeRatio;
        clear();
        addDeletedPairsListener(new DeletedPairsListener<V>() {
            @Override
            public void objectRemoved(long key, V value) {
                pushedOutValue = value;
            }
        });
    }

    @Override
    public void clear() {
        firstGenerationQueue = new LongLinkedHashMap<V>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Long, V> eldest) {
                final boolean result = size() + secondGenerationQueue.size() > LongObjectCache.this.size;
                if (result) {
                    fireListenersAboutDeletion(eldest.getKey(), eldest.getValue());
                }
                return result;
            }
        };
        final int secondGenSizeBound = (int) (size * secondGenSizeRatio);
        secondGenerationQueue = new LongLinkedHashMap<V>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Long, V> eldest) {
                final boolean result = size() > secondGenSizeBound;
                if (result) {
                    --size;
                    firstGenerationQueue.put(eldest.getKey(), eldest.getValue());
                    ++size;
                }
                return result;
            }
        };
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    // returns value pushed out of the cache

    @Override
    public V remove(final long key) {
        V x = firstGenerationQueue.remove(key);
        if (x != null) {
            fireListenersAboutDeletion(key, x);
        } else {
            x = secondGenerationQueue.remove(key);
            if (x != null) {
                fireListenersAboutDeletion(key, x);
            }
        }
        return x;
    }

    public void removeAll() {
        for (final long key : firstGenerationQueue.keySet()) {
            fireListenersAboutDeletion(key, firstGenerationQueue.get(key));
        }
        for (final long key : secondGenerationQueue.keySet()) {
            fireListenersAboutDeletion(key, secondGenerationQueue.get(key));
        }
        clear();
    }

    // returns value pushed out of the cache

    @Override
    public V cacheObject(final long key, @NotNull final V x) {
        pushedOutValue = null;
        if (firstGenerationQueue.put(key, x) == null) {
            secondGenerationQueue.remove(key);
        }
        return pushedOutValue;
    }

    @Override
    public V tryKey(final long key) {
        incAttempts();
        V result = secondGenerationQueue.get(key);
        if (result == null) {
            result = firstGenerationQueue.remove(key);
            if (result != null) {
                secondGenerationQueue.put(key, result);
            }
        }
        if (result != null) {
            incHits();
        }
        return result;
    }

    /**
     * @param key key.
     * @return object from the cache not affecting usages statistics.
     */
    @Override
    public V getObject(final long key) {
        V result = firstGenerationQueue.get(key);
        if (result == null) {
            result = secondGenerationQueue.get(key);
        }
        return result;
    }

    @Override
    public int count() {
        return firstGenerationQueue.size() + secondGenerationQueue.size();
    }

    public Iterator<Long> keys() {
        return new LongObjectCacheKeysIterator<>(this);
    }

    public Iterator<V> values() {
        return new LongObjectCacheValuesIterator<>(this);
    }

    public boolean forEachEntry(final ObjectProcedure<Map.Entry<Long, V>> procedure) {
        for (final Map.Entry<Long, V> entry : firstGenerationQueue.entrySet()) {
            if (!procedure.execute(entry)) return false;
        }
        for (final Map.Entry<Long, V> entry : secondGenerationQueue.entrySet()) {
            if (!procedure.execute(entry)) return false;
        }
        return true;
    }

    protected static class LongObjectCacheKeysIterator<V> implements Iterator<Long> {
        private final Iterator<Long> firstGenIterator;
        private final Iterator<Long> secondGenIterator;

        protected LongObjectCacheKeysIterator(final LongObjectCache<V> cache) {
            firstGenIterator = cache.firstGenerationQueue.keySet().iterator();
            secondGenIterator = cache.secondGenerationQueue.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return firstGenIterator.hasNext() || secondGenIterator.hasNext();
        }

        @Override
        public Long next() {
            return (firstGenIterator.hasNext()) ? firstGenIterator.next() : secondGenIterator.next();
        }

        @Override
        public void remove() {
            if (firstGenIterator.hasNext()) {
                firstGenIterator.remove();
            } else {
                secondGenIterator.remove();
            }
        }
    }

    protected static class LongObjectCacheValuesIterator<V> implements Iterator<V> {
        private final Iterator<V> firstGenIterator;
        private final Iterator<V> secondGenIterator;

        protected LongObjectCacheValuesIterator(final LongObjectCache<V> cache) {
            firstGenIterator = cache.firstGenerationQueue.values().iterator();
            secondGenIterator = cache.secondGenerationQueue.values().iterator();
        }

        @Override
        public boolean hasNext() {
            return firstGenIterator.hasNext() || secondGenIterator.hasNext();
        }

        @Override
        public V next() {
            return (firstGenIterator.hasNext()) ? firstGenIterator.next() : secondGenIterator.next();
        }

        @Override
        public void remove() {
            if (firstGenIterator.hasNext()) {
                firstGenIterator.remove();
            } else {
                secondGenIterator.remove();
            }
        }
    }

    // start of listening features

    public interface DeletedPairsListener<V> extends EventListener {
        void objectRemoved(long key, V value);
    }

    public void addDeletedPairsListener(final DeletedPairsListener<V> listener) {
        if (listeners == null) {
            listeners = new DeletedPairsListener[1];
        } else {
            final DeletedPairsListener<V>[] newListeners = new DeletedPairsListener[listeners.length + 1];
            System.arraycopy(listeners, 0, newListeners, 0, listeners.length);
            listeners = newListeners;
        }
        listeners[listeners.length - 1] = listener;
    }

    public void removeDeletedPairsListener(final DeletedPairsListener<V> listener) {
        if (listeners != null) {
            if (listeners.length == 1) {
                listeners = null;
            } else {
                final DeletedPairsListener<V>[] newListeners = new DeletedPairsListener[listeners.length - 1];
                int i = 0;
                for (final DeletedPairsListener<V> myListener : listeners) {
                    if (myListener != listener) {
                        newListeners[i++] = myListener;
                    }
                }
                listeners = newListeners;
            }
        }
    }

    private void fireListenersAboutDeletion(final long key, final V x) {
        if (listeners != null) {
            for (final DeletedPairsListener<V> myListener : listeners) {
                myListener.objectRemoved(key, x);
            }
        }
    }

    // end of listening features
}
