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

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public interface PersistentLongMap<V> {

    ImmutableMap<V> beginRead();

    PersistentLongMap<V> getClone();

    MutableMap<V> beginWrite();

    interface ImmutableMap<V> extends Iterable<Entry<V>> {
        V get(long key);

        boolean containsKey(long key);

        boolean isEmpty();

        int size();

        Entry<V> getMinimum();

        Iterator<Entry<V>> reverseIterator();

        Iterator<Entry<V>> tailEntryIterator(long staringKey);

        Iterator<Entry<V>> tailReverseEntryIterator(long staringKey);
    }

    interface MutableMap<V> extends ImmutableMap<V> {
        void put(long key, @NotNull V value);

        V remove(long key);

        boolean endWrite();

        void testConsistency(); // for testing consistency
    }

    interface Entry<V> extends Comparable<Entry<V>> {
        long getKey();

        V getValue();
    }
}
