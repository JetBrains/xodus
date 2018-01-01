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
package jetbrains.exodus.util

import java.lang.Integer.MAX_VALUE
import java.util.concurrent.atomic.AtomicInteger

const val prime = 800076929 // 800076929 is a prime

class IdGenerator {

    private val id = AtomicInteger((Math.random() * MAX_VALUE).toInt())

    fun nextId(): Int {
        while (true) {
            val currentId = id.get()
            var nextId = (currentId + prime) and 0x7fffffff
            if (nextId == 0) nextId = prime
            if (id.compareAndSet(currentId, nextId)) {
                return nextId
            }
        }
    }
}