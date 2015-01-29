/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StablePriorityQueue<P extends Comparable<? super P>, E> extends PriorityQueue<P, E> {

    private final TreeMap<P, LinkedHashSet<E>> theQueue;
    private final Map<E, Pair<E, P>> priorities;
    private final Lock lock;

    public StablePriorityQueue() {
        theQueue = new TreeMap<P, LinkedHashSet<E>>();
        priorities = new HashMap<E, Pair<E, P>>();
        lock = new ReentrantLock();
    }

    @Override
    public boolean isEmpty() {
        lock();
        try {
            return priorities.isEmpty();
        } finally {
            unlock();
        }
    }

    @Override
    public int size() {
        lock();
        try {
            return priorities.size();
        } finally {
            unlock();
        }
    }

    @Override
    public E push(@NotNull final P priority, @NotNull final E value) {
        LinkedHashSet<E> values;
        Pair<E, P> oldPair = priorities.put(value, new Pair<E, P>(value, priority));
        final P oldPriority = oldPair == null ? null : oldPair.getSecond();
        final E oldValue = oldPair == null ? null : oldPair.getFirst();
        if (oldPriority != null && (values = theQueue.get(oldPriority)) != null) {
            values.remove(value);
            if (values.isEmpty()) {
                theQueue.remove(oldPriority);
            }
        }
        values = theQueue.get(priority);
        if (values == null) {
            values = new LinkedHashSet<E>();
            theQueue.put(priority, values);
        }
        values.add(value);
        return oldValue;
    }

    @Override
    public Pair<P, E> peekPair() {
        if (isEmpty()) {
            return null;
        }
        final TreeMap<P, LinkedHashSet<E>> queue = theQueue;
        final P priority = queue.lastKey();
        final LinkedHashSet<E> values = queue.get(priority);
        return new Pair<P, E>(priority, values.getBack());
    }

    @Override
    public Pair<P, E> floorPair() {
        if (isEmpty()) {
            return null;
        }
        final TreeMap<P, LinkedHashSet<E>> queue = theQueue;
        final P priority = queue.firstKey();
        final LinkedHashSet<E> values = queue.get(priority);
        return new Pair<P, E>(priority, values.getTop());
    }

    @Override
    public E pop() {
        if (isEmpty()) {
            return null;
        }
        final TreeMap<P, LinkedHashSet<E>> queue = theQueue;
        final P priority = queue.lastKey();
        final Set<E> values = queue.get(priority);
        final E result = values.iterator().next();
        priorities.remove(result);
        values.remove(result);
        if (values.isEmpty()) {
            queue.remove(priority);
        }
        return result;
    }

    @Override
    public void clear() {
        theQueue.clear();
        priorities.clear();
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
    public Iterator<E> iterator() {
        return priorities.keySet().iterator();
    }


    public boolean remove(@NotNull final E value) {
        final Pair<E, P> pair = priorities.remove(value);
        if (pair == null) {
            return false;
        }
        final P priority = pair.getSecond();
        final LinkedHashSet<E> values = theQueue.get(priority);
        values.remove(value);
        if (values.isEmpty()) {
            theQueue.remove(priority);
        }
        return true;
    }
}
