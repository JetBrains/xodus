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
package jetbrains.exodus.core.dataStructures.decorators;

import java.util.*;

public class QueueDecorator<E> implements Queue<E> {

    private List<E> decorated;

    public QueueDecorator() {
        clear();
    }

    public QueueDecorator(Collection<? extends E> c) {
        this();
        addAll(c);
    }

    @Override
    public int size() {
        return decorated.size();
    }

    @Override
    public boolean isEmpty() {
        return decorated == Collections.emptyList() || decorated.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return decorated.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return decorated.iterator();
    }

    @Override
    public Object[] toArray() {
        return decorated.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return decorated.toArray(a);
    }

    @Override
    public boolean add(E e) {
        checkDecorated();
        return decorated.add(e);
    }

    @Override
    public boolean remove(Object o) {
        checkDecorated();
        return decorated.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return decorated.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        checkDecorated();
        return decorated.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        checkDecorated();
        return decorated.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        checkDecorated();
        return decorated.retainAll(c);
    }

    @Override
    public void clear() {
        decorated = Collections.emptyList();
    }

    @Override
    public boolean offer(E e) {
        checkDecorated();
        return decorated.add(e);
    }

    @Override
    public E remove() {
        if (decorated.size() == 0) {
            throw new NoSuchElementException();
        }
        return decorated.remove(0);
    }

    @Override
    public E poll() {
        return (decorated.size() == 0) ? null : decorated.remove(0);
    }

    @Override
    public E element() {
        if (decorated.size() == 0) {
            throw new NoSuchElementException();
        }
        return decorated.get(0);
    }

    @Override
    public E peek() {
        return (decorated.size() == 0) ? null : decorated.get(0);
    }

    private void checkDecorated() {
        if (decorated == Collections.emptyList()) {
            decorated = new ArrayList<>();
        }
    }
}
