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
package jetbrains.exodus.bindings;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

public class ComparableSet<T extends Comparable<T>> implements Comparable<ComparableSet<T>>, Iterable<T> {

    @NotNull
    private final NavigableSet<T> set;
    private boolean isDirty;

    public ComparableSet() {
        this.set = new TreeSet<>();
        isDirty = false;
    }

    public ComparableSet(@NotNull final Iterable<T> it) {
        this.set = new TreeSet<>();
        for (final T item : it) {
            set.add(item);
        }
        isDirty = false;
    }

    @Override
    public int compareTo(@NotNull final ComparableSet<T> right) {
        //noinspection ConstantConditions
        if (isEmpty() || !getItemClass().equals(String.class)) {
            return caseSensitiveCompareTo(right);
        }
        return caseInsensitiveCompareTo(right);
    }

    public T getMinimum() {
        return set.first();
    }

    public T getMaximum() {
        return set.last();
    }

    /**
     * @return difference between this set and subtrahend.
     */
    public ComparableSet<T> minus(@Nullable final ComparableSet<T> subtrahend) {
        if (subtrahend == null) {
            return this;
        }
        final ComparableSet<T> result = new ComparableSet<>();
        for (final T item : set) {
            if (!subtrahend.set.contains(item)) {
                result.addItem(item);
            }
        }
        return result;
    }

    public boolean containsItem(@NotNull final T item) {
        return set.contains(item);
    }

    public boolean addItem(@NotNull final T item) {
        final boolean result = set.add(item);
        isDirty |= result;
        return result;
    }

    public boolean removeItem(@NotNull final T item) {
        final boolean result = set.remove(item);
        isDirty |= result;
        return result;
    }

    public int size() {
        return set.size();
    }

    public boolean isEmpty() {
        return set.isEmpty();
    }

    public boolean isDirty() {
        return isDirty;
    }

    public void forEach(@NotNull final Consumer<T> action) {
        int index = 0;
        for (final T item : set) {
            action.accept(item, index++);
        }
    }

    public T[] toArray() {
        //noinspection unchecked
        return set.toArray((T[]) new Comparable[size()]);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return o != null && set.equals(((ComparableSet<?>) o).set);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public String toString() {
        return "ComparableSet" + set;
    }

    @Nullable
    public Class<? extends Comparable> getItemClass() {
        final Iterator<T> it = set.iterator();
        return it.hasNext() ? it.next().getClass() : null;
    }

    void setIsDirty(final boolean isDirty) {
        this.isDirty = isDirty;
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return set.iterator();
    }

    private int caseSensitiveCompareTo(@NotNull ComparableSet<T> right) {
        final Iterator<T> thisIt = set.iterator();
        final Iterator<T> rightIt = right.set.iterator();
        while (thisIt.hasNext() && rightIt.hasNext()) {
            final int cmp = thisIt.next().compareTo(rightIt.next());
            if (cmp != 0) {
                return cmp;
            }
        }
        if (thisIt.hasNext()) {
            return 1;
        }
        if (rightIt.hasNext()) {
            return -1;
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private int caseInsensitiveCompareTo(@NotNull ComparableSet<T> right) {
        final Iterator<String> thisIt = (Iterator<String>) set.iterator();
        final Iterator<String> rightIt = (Iterator<String>) right.set.iterator();
        while (thisIt.hasNext() && rightIt.hasNext()) {
            final int cmp = thisIt.next().compareToIgnoreCase(rightIt.next());
            if (cmp != 0) {
                return cmp;
            }
        }
        if (thisIt.hasNext()) {
            return 1;
        }
        if (rightIt.hasNext()) {
            return -1;
        }
        return 0;
    }

    public interface Consumer<T extends Comparable<T>> {

        void accept(@NotNull final T item, final int index);
    }
}
