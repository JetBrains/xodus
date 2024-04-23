/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIdSetIterable
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.query.metadata.ModelMetaData
import java.util.*
import kotlin.collections.HashSet

@Suppress("EqualsOrHashCode")
class Or(left: NodeBase, right: NodeBase) : CommutativeOperator(left, right) {

    companion object {

        @JvmStatic
        fun or(left: NodeBase, right: NodeBase): NodeBase {
            if (left is GetAll) {
                return left
            }
            if (right is GetAll) {
                return right
            }
            return Or(left, right)
        }
    }

    override fun instantiate(
        entityType: String,
        queryEngine: QueryEngine,
        metaData: ModelMetaData?,
        context: InstantiateContext
    ): Iterable<Entity> {
        val leftInstance = left.instantiate(entityType, queryEngine, metaData, context);
        val rightInstance = right.instantiate(entityType, queryEngine, metaData, context);
        if (leftInstance is EntityIterable && rightInstance is EntityIterable){
            return leftInstance.union(rightInstance)
        } else {
            return leftInstance.union(rightInstance)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) {
            return true
        }
        if (other == null) {
            return false
        }
        checkWildcard(other)
        return other is Or && super.equals(other)
    }

    override fun getClone(): NodeBase {
        return Or(left.clone, right.clone)
    }

    override fun getSimpleName(): String {
        return "or"
    }
}
