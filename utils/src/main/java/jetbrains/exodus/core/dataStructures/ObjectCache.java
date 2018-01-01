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

import jetbrains.exodus.core.dataStructures.hash.LinkedHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.EventListener;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("unchecked")
public class ObjectCache<K, V> extends ObjectCacheBase<K, V> {

    public static final float DEFAULT_SECOND_GENERATION_QUEUE_SIZE_RATIO = 0.4f;

    private final Lock lock;
    private final float secondGenSizeRatio;
    private LinkedHashMap<K, V> firstGenerationQueue;
    private LinkedHashMap<K, V> secondGenerationQueue;
    private DeletedPairsListener<K, V>[] listeners;
    private V pushedOutValue;

    public ObjectCache() {
        this(DEFAULT_SIZE);
    }

    public ObjectCache(int cacheSize) {
        this(cacheSize, DEFAULT_SECOND_GENERATION_QUEUE_SIZE_RATIO);
    }

    @SuppressWarnings({"unchecked"})
    public ObjectCache(final int cacheSize, float secondGenSizeRatio) {
        super(cacheSize);
        lock = new ReentrantLock();
        if (secondGenSizeRatio < 0.05f) {
            secondGenSizeRatio = 0.05f;
        } else if (secondGenSizeRatio > 0.95f) {
            secondGenSizeRatio = 0.95f;
        }
        this.secondGenSizeRatio = secondGenSizeRatio;
        clear();
        addDeletedPairsListener(new DeletedPairsListener<K, V>() {
            @Override
            public void objectRemoved(K key, V value) {
                pushedOutValue = value;
            }
        });
    }

    public void clear() {
        if (firstGenerationQueue != null && secondGenerationQueue != null && isEmpty()) {
            return;
        }
        firstGenerationQueue = new LinkedHashMap<K, V>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
                final boolean result = size() + secondGenerationQueue.size() > ObjectCache.this.size;
                if (result) {
                    fireListenersAboutDeletion(eldest.getKey(), eldest.getValue());
                }
                return result;
            }
        };
        final int secondGenSizeBound = (int) (size * secondGenSizeRatio);
        secondGenerationQueue = new LinkedHashMap<K, V>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
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

    @Override
    public V remove(@NotNull final K key) {
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
        for (final Map.Entry<K, V> entry : firstGenerationQueue.entrySet()) {
            fireListenersAboutDeletion(entry.getKey(), entry.getValue());
        }
        for (final Map.Entry<K, V> entry : secondGenerationQueue.entrySet()) {
            fireListenersAboutDeletion(entry.getKey(), entry.getValue());
        }
        clear();
    }

    // returns value pushed out of the cache

    @Override
    public V cacheObject(@NotNull final K key, @NotNull final V x) {
        pushedOutValue = null;
        if (firstGenerationQueue.put(key, x) == null) {
            secondGenerationQueue.remove(key);
        }
        return pushedOutValue;
    }

    @Override
    public V tryKey(@NotNull final K key) {
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
    public V getObject(@NotNull final K key) {
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

    public Iterator<K> keys() {
        return new ObjectCacheKeysIterator<>(this);
    }

    public Iterator<V> values() {
        return new ObjectCacheValuesIterator<>(this);
    }

    protected static class ObjectCacheKeysIterator<K, V> implements Iterator<K> {
        private final Iterator<K> firstGenIterator;
        private final Iterator<K> secondGenIterator;

        protected ObjectCacheKeysIterator(final ObjectCache<K, V> cache) {
            firstGenIterator = cache.firstGenerationQueue.keySet().iterator();
            secondGenIterator = cache.secondGenerationQueue.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return firstGenIterator.hasNext() || secondGenIterator.hasNext();
        }

        @Override
        public K next() {
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

    protected static class ObjectCacheValuesIterator<K, V> implements Iterator<V> {
        private final Iterator<V> firstGenIterator;
        private final Iterator<V> secondGenIterator;

        protected ObjectCacheValuesIterator(final ObjectCache<K, V> cache) {
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

    public interface DeletedPairsListener<K, V> extends EventListener {
        void objectRemoved(K key, V value);
    }

    public void addDeletedPairsListener(final DeletedPairsListener<K, V> listener) {
        if (listeners == null) {
            listeners = new DeletedPairsListener[1];
        } else {
            final DeletedPairsListener<K, V>[] newListeners = new DeletedPairsListener[listeners.length + 1];
            System.arraycopy(listeners, 0, newListeners, 0, listeners.length);
            listeners = newListeners;
        }
        listeners[listeners.length - 1] = listener;
    }

    public void removeDeletedPairsListener(final DeletedPairsListener<K, V> listener) {
        if (listeners != null) {
            if (listeners.length == 1) {
                listeners = null;
            } else {
                final DeletedPairsListener<K, V>[] newListeners = new DeletedPairsListener[listeners.length - 1];
                int i = 0;
                for (final DeletedPairsListener<K, V> myListener : listeners) {
                    if (myListener != listener) {
                        newListeners[i++] = myListener;
                    }
                }
                listeners = newListeners;
            }
        }
    }

    private void fireListenersAboutDeletion(final K key, final V x) {
        if (listeners != null) {
            for (final DeletedPairsListener<K, V> myListener : listeners) {
                myListener.objectRemoved(key, x);
            }
        }
    }

    // end of listening features
}
