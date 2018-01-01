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

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet;
import jetbrains.exodus.core.execution.locks.CriticalSection;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StablePriorityQueue<P extends Comparable<? super P>, E> extends PriorityQueue<P, E> {

    private final TreeMap<P, LinkedHashSet<E>> theQueue;
    private final Map<E, Pair<E, P>> priorities;
    private final AtomicInteger size;
    private final CriticalSection criticalSection;

    public StablePriorityQueue() {
        theQueue = new TreeMap<>();
        priorities = new HashMap<>();
        size = new AtomicInteger(0);
        criticalSection = new CriticalSection();
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public E push(@NotNull final P priority, @NotNull final E value) {
        LinkedHashSet<E> values;
        final Pair<E, P> oldPair = priorities.remove(value);
        priorities.put(value, new Pair<>(value, priority));
        invalidateSize();
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
            values = new LinkedHashSet<>();
            theQueue.put(priority, values);
        }
        values.add(value);
        return oldValue;
    }

    @Override
    public Pair<P, E> peekPair() {
        if (priorities.size() == 0) {
            return null;
        }
        final TreeMap<P, LinkedHashSet<E>> queue = theQueue;
        final P priority = queue.lastKey();
        final LinkedHashSet<E> values = queue.get(priority);
        return new Pair<>(priority, values.getBack());
    }

    @Override
    public Pair<P, E> floorPair() {
        if (priorities.size() == 0) {
            return null;
        }
        final TreeMap<P, LinkedHashSet<E>> queue = theQueue;
        final P priority = queue.firstKey();
        final LinkedHashSet<E> values = queue.get(priority);
        return new Pair<>(priority, values.getTop());
    }

    @Override
    public E pop() {
        if (priorities.size() == 0) {
            return null;
        }
        final TreeMap<P, LinkedHashSet<E>> queue = theQueue;
        final P priority = queue.lastKey();
        final Set<E> values = queue.get(priority);
        final E result = values.iterator().next();
        priorities.remove(result);
        invalidateSize();
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
        size.set(0);
    }

    @Override
    public CriticalSection lock() {
        return criticalSection.enter();
    }

    @Override
    public void unlock() {
        criticalSection.unlock();
    }

    @Override
    public Iterator<E> iterator() {
        return new QueueIterator();
    }

    public boolean remove(@NotNull final E value) {
        final Pair<E, P> pair = priorities.remove(value);
        if (pair == null) {
            return false;
        }
        invalidateSize();
        final P priority = pair.getSecond();
        final LinkedHashSet<E> values = theQueue.get(priority);
        values.remove(value);
        if (values.isEmpty()) {
            theQueue.remove(priority);
        }
        return true;
    }


    private void invalidateSize() {
        size.set(priorities.size());
    }

    private class QueueIterator implements Iterator<E> {

        @NotNull
        private final Iterator<Map.Entry<P, LinkedHashSet<E>>> priorityIt;
        @NotNull
        private Iterator<E> currentIt;

        private QueueIterator() {
            priorityIt = theQueue.entrySet().iterator();
            //noinspection unchecked
            currentIt = Collections.EMPTY_LIST.iterator();
            checkCurrentIterator();
        }

        @Override
        public boolean hasNext() {
            return currentIt.hasNext();
        }

        @Override
        public E next() {
            final E result = currentIt.next();
            checkCurrentIterator();
            return result;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void checkCurrentIterator() {
            while (!currentIt.hasNext()) {
                if (!priorityIt.hasNext()) {
                    break;
                }
                final Map.Entry<P, LinkedHashSet<E>> next = priorityIt.next();
                currentIt = next.getValue().iterator();
            }
        }
    }
}
