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

import jetbrains.exodus.core.dataStructures.hash.HashSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;


public class HashSetDecorator<E> implements Set<E> {

    private Set<E> decorated;

    public HashSetDecorator() {
        clear();
    }

    public HashSetDecorator(Collection<? extends E> c) {
        this();
        addAll(c);
    }

    @Override
    public int size() {
        return decorated.size();
    }

    @Override
    public boolean isEmpty() {
        return decorated == Collections.emptySet() || decorated.isEmpty();
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
        if (decorated == Collections.emptySet()) return false;
        final boolean result = decorated.remove(o);
        if (result && decorated.isEmpty()) {
            clear();
        }
        return result;
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
    public boolean retainAll(Collection<?> c) {
        checkDecorated();
        return decorated.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        if (decorated == Collections.emptySet()) return false;
        final boolean result = decorated.removeAll(c);
        if (result && decorated.isEmpty()) {
            clear();
        }
        return result;
    }

    @Override
    public void clear() {
        decorated = Collections.emptySet();
    }

    private void checkDecorated() {
        if (decorated == Collections.emptySet()) {
            decorated = new HashSet<>();
        }
    }
}
