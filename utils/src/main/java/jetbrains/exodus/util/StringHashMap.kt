/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.util

import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.util.StringInterner.Companion.intern

class StringHashMap<T> : HashMap<String?, T?> {
    constructor() : super(
        DEFAULT_CAPACITY,
        0,
        DEFAULT_LOAD_FACTOR,
        HashMap.Companion.DEFAULT_TABLE_SIZE,
        HashMap.Companion.DEFAULT_MASK
    )

    constructor(capacity: Int, loadFactor: Float) : super(capacity, loadFactor)

    override fun put(key: String?, value: T?): T? {
        return super.put(intern(key), value)
    }

    companion object {
        private const val DEFAULT_CAPACITY = 9
        private const val DEFAULT_LOAD_FACTOR = 3f
    }
}
