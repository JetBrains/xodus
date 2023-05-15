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
package jetbrains.exodus.core.dataStructures

class Stack<T> : ArrayList<T>() {
    private var last: T? = null
    fun push(t: T) {
        if (last != null) {
            add(last!!)
        }
        last = t
    }

    fun peek(): T? {
        return last
    }

    fun pop(): T? {
        val result = last
        if (result != null) {
            last = if (super.isEmpty()) null else removeAt(super.size - 1)
        }
        return result
    }

    override fun size(): Int {
        return if (last == null) 0 else super.size + 1
    }

    override fun isEmpty(): Boolean {
        return last == null
    }
}
