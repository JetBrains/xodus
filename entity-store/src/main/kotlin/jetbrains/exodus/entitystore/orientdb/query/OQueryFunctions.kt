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
package jetbrains.exodus.entitystore.orientdb.query

import jetbrains.exodus.entitystore.orientdb.OStoreTransaction

object OQueryFunctions {

    fun intersect(left: OSelect, right: OSelect): OSelect {
        return when {
            left is ORecordIdSelect && right is ORecordIdSelect -> {
                ensureLimitIsNotUsed(left, right)
                ensureSkipIsNotUsed(left, right)

                val newOrder = left.order.merge(right.order)
                val ids = left.recordIds.intersect(right.recordIds.toSet())
                ORecordIdSelect(ids, newOrder)
            }

            left is OClassSelect && right is OClassSelect && isSameClassName(left, right) -> {
                ensureInvariants(left, right)
                val newCondition = left.condition.and(right.condition)
                val newOrder = left.order.merge(right.order)
                OClassSelect(left.className, newCondition, newOrder)
            }

            else -> {
                OIntersectSelect(left, right)
            }
        }
    }

    fun union(left: OSelect, right: OSelect): OSelect {
        return when {
            left is ORecordIdSelect && right is ORecordIdSelect -> {
                ensureLimitIsNotUsed(left, right)
                ensureSkipIsNotUsed(left, right)

                val newOrder = left.order.merge(right.order)
                val ids = (left.recordIds + right.recordIds).toSet()
                ORecordIdSelect(ids, newOrder)
            }

            left is OClassSelect && right is OClassSelect && isSameClassName(left, right) -> {
                ensureInvariants(left, right)
                val newCondition = left.condition.or(right.condition)
                val newOrder = left.order.merge(right.order)
                OClassSelect(left.className, newCondition, newOrder)
            }

            else -> {
                OUnionSelect(left, right)
            }
        }
    }

    fun difference(left: OSelect, right: OSelect): OSelect {
        return when {
            left is ORecordIdSelect && right is ORecordIdSelect -> {
                ensureLimitIsNotUsed(left, right)
                ensureSkipIsNotUsed(left, right)

                val newOrder = left.order.merge(right.order)
                val ids = left.recordIds - right.recordIds.toSet()
                ORecordIdSelect(ids, newOrder)
            }

            left is OClassSelect && right is OClassSelect && isSameClassName(left, right) -> {
                ensureInvariants(left, right)
                val newCondition = left.condition.andNot(right.condition)
                val newOrder = left.order.merge(right.order)
                OClassSelect(left.className, newCondition, newOrder)
            }

            else -> {
                ODifferenceSelect(left, right)
            }
        }
    }

    fun distinct(source: OSelect): OSelect {
        return ODistinctSelect(source)
    }

    fun reverse(query: OSelect): OSelect {
        val order = query.order?.reverse() ?: return query
        return query.withOrder(order)
    }

    private fun ensureInvariants(left: OClassSelect, right: OClassSelect) {
        ensureSkipIsNotUsed(left, right)
        ensureLimitIsNotUsed(left, right)
    }

    private fun ensureSkipIsNotUsed(left: OSelect, right: OSelect) {
        val lazyMessage = { "Skip can not be used for sub-query" }
        check(left.skip == null, lazyMessage)
        check(right.skip == null, lazyMessage)
    }

    private fun ensureLimitIsNotUsed(left: OSelect, right: OSelect) {
        val lazyMessage = { "Take can not be used for sub-query" }
        check(left.limit == null, lazyMessage)
        check(right.limit == null, lazyMessage)
    }

    private fun isSameClassName(left: OClassSelect, right: OClassSelect): Boolean {
        return left.className == right.className
    }
}

class OCountSelect(
    val source: OSelect,
) : OQuery {

    override fun sql(builder: SqlBuilder) {
        builder.append("SELECT count(*) as count FROM (")
        source.sql(builder)
        builder.append(")")
    }

    fun count(tx: OStoreTransaction): Long = OQueryExecution.execute(this, tx).next().getProperty("count")
}

class OFirstSelect(
    val source: OSelect,
) : OQuery {

    override fun sql(builder: SqlBuilder) {
        val index = builder.nextVarIndex()
        builder.append("SELECT expand(first(\$a${index})) LET \$a${index} = (")
        source.sql(builder)
        builder.append(")")
    }
}


class OLastSelect(
    val source: OSelect,
) : OQuery {

    override fun sql(builder: SqlBuilder) {
        val index = builder.nextVarIndex()
        builder.append("SELECT expand(last(\$a${index})) LET \$a${index} = (")
        source.sql(builder)
        builder.append(")")
    }
}
