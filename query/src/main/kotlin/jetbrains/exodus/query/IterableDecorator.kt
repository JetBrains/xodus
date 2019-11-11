/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.query.metadata.ModelMetaData

class IterableDecorator(private val it: Iterable<Entity>) : NodeBase() {

    override fun instantiate(entityType: String, queryEngine: QueryEngine, metaData: ModelMetaData): Iterable<Entity> {
        return it
    }

    override fun getClone() = IterableDecorator(it)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        other?.let { right ->
            return right is IterableDecorator && it === right.it
        }
        return false
    }

    override fun getHandle(sb: StringBuilder): StringBuilder {
        return super.getHandle(sb).append('(').append(it.hashCode() and 0x7fffffff).append(')')
    }

    override fun getSimpleName() = "id"
}