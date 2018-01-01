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

import org.jetbrains.annotations.Nullable;

public class LongMapEntry<V> implements PersistentLongMap.Entry<V>, LongComparable<PersistentLongMap.Entry<V>> {

    private final long key;
    private final V value;

    public LongMapEntry(long k) {
        this(k, null);
    }

    public LongMapEntry(long k, @Nullable V v) {
        key = k;
        value = v;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public long getWeight() {
        return key;
    }

    @SuppressWarnings("NullableProblems") // Comparable<T> contract requires NPE if argument is null
    @Override
    public int compareTo(PersistentLongMap.Entry<V> o) {
        final long otherKey = o.getKey();
        return key > otherKey ? 1 : key == otherKey ? 0 : -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PersistentLongMap.Entry)) return false;

        final PersistentLongMap.Entry that = (PersistentLongMap.Entry) o;

        return key == that.getKey() && (value != null ? value.equals(that.getValue()) : that.getValue() == null);
    }

    @Override
    public int hashCode() {
        int result = (int) (key ^ (key >>> 32));
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
