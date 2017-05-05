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
package jetbrains.exodus.core.dataStructures.persistent;

import org.jetbrains.annotations.Nullable;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
class LongMapEntry<V> implements PersistentLongMap.Entry<V> {

    private final long key;
    private final V value;

    LongMapEntry(long k) {
        this(k, null);
    }

    LongMapEntry(long k, @Nullable V v) {
        key = k;
        value = v;
    }

    @SuppressWarnings("NullableProblems") // Comparable<T> contract requires NPE if argument is null
    @Override
    public int compareTo(PersistentLongMap.Entry<V> o) {
        final long otherKey = o.getKey();
        return key > otherKey ? 1 : key == otherKey ? 0 : -1;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getKey() {
        return key;
    }
}
