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
package jetbrains.exodus.core.dataStructures.persistent

class LongMapEntry<V> @JvmOverloads constructor(override val weight: Long, override val value: V? = null) :
    PersistentLongMap.Entry<V?>, LongComparable<PersistentLongMap.Entry<V>?> {

    // Comparable<T> contract requires NPE if argument is null
    override operator fun compareTo(o: PersistentLongMap.Entry<V>): Int {
        val otherKey = o.key
        return if (weight > otherKey) 1 else if (weight == otherKey) 0 else -1
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o !is PersistentLongMap.Entry<*>) return false
        val that = o
        return weight == that.key && if (value != null) value == that.value else that.value == null
    }

    override fun hashCode(): Int {
        var result = (weight xor (weight ushr 32)).toInt()
        result = 31 * result + (value?.hashCode() ?: 0)
        return result
    }
}
