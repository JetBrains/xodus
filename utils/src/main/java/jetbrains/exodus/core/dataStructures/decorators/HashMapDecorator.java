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

import jetbrains.exodus.core.dataStructures.hash.HashMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class HashMapDecorator<K, V> implements Map<K, V> {

    private Map<K, V> decorated;

    public HashMapDecorator() {
        clear();
    }

    @Override
    public int size() {
        return decorated.size();
    }

    @Override
    public boolean isEmpty() {
        return decorated == Collections.emptyMap() || decorated.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return decorated.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return decorated.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return decorated.get(key);
    }

    @Override
    public V put(K key, V value) {
        checkDecorated();
        return decorated.put(key, value);
    }

    @Override
    public V remove(Object key) {
        if (decorated == Collections.emptyMap()) return null;
        final V result = decorated.remove(key);
        if (result != null && decorated.isEmpty()) {
            clear();
        }
        return result;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        checkDecorated();
        decorated.putAll(m);
    }

    @Override
    public void clear() {
        decorated = Collections.emptyMap();
    }

    @Override
    public Set<K> keySet() {
        return decorated.keySet();
    }

    @Override
    public Collection<V> values() {
        return decorated.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return decorated.entrySet();
    }

    private void checkDecorated() {
        if (decorated == Collections.emptyMap()) {
            decorated = new HashMap<>();
        }
    }
}
