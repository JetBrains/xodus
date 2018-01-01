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
package jetbrains.exodus.util;

import jetbrains.exodus.core.dataStructures.hash.HashMap;

public class StringHashMap<T> extends HashMap<String, T> {
    private static final int DEFAULT_CAPACITY = 9;
    private static final float DEFAULT_LOAD_FACTOR = 3;

    public StringHashMap() {
        super(DEFAULT_CAPACITY, 0, DEFAULT_LOAD_FACTOR, DEFAULT_TABLE_SIZE, DEFAULT_MASK);
    }

    public StringHashMap(int capacity, float loadFactor) {
        super(capacity, loadFactor);
    }

    @Override
    public T put(final String key, final T value) {
        return super.put(StringInterner.intern(key), value);
    }
}
