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
package jetbrains.exodus.entitystore.youtrackdb.query

import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction

object YTDBQueryFunctions {

    fun intersect(left: YTDBSelect, right: YTDBSelect): YTDBSelect {
        return when {
            left is YTDBRecordIdSelect && right is YTDBRecordIdSelect -> {
                ensureLimitIsNotUsed(left, right)
                ensureSkipIsNotUsed(left, right)

                val newOrder = left.order.merge(right.order)
                val ids = left.recordIds.intersect(right.recordIds.toSet())
                YTDBRecordIdSelect(ids, newOrder)
            }

            left is YTDBClassSelect && right is YTDBClassSelect && isSameClassName(left, right) -> {
                ensureInvariants(left, right)
                val newCondition = left.condition.and(right.condition)
                val newOrder = left.order.merge(right.order)
                YTDBClassSelect(left.className, newCondition, newOrder)
            }

            else -> {
                YTDBIntersectSelect(left, right)
            }
        }
    }

    fun union(left: YTDBSelect, right: YTDBSelect): YTDBSelect {
        return when {
            left is YTDBRecordIdSelect && right is YTDBRecordIdSelect -> {
                ensureLimitIsNotUsed(left, right)
                ensureSkipIsNotUsed(left, right)

                val newOrder = left.order.merge(right.order)
                val ids = (left.recordIds + right.recordIds).toSet()
                YTDBRecordIdSelect(ids, newOrder)
            }

            left is YTDBClassSelect && right is YTDBClassSelect && isSameClassName(left, right) -> {
                ensureInvariants(left, right)
                val newCondition = left.condition.or(right.condition)
                val newOrder = left.order.merge(right.order)
                YTDBClassSelect(left.className, newCondition, newOrder)
            }

            else -> {
                YTDBUnionSelect(left, right)
            }
        }
    }

    fun difference(left: YTDBSelect, right: YTDBSelect): YTDBSelect {
        return when {

            left is YTDBClassSelect && right is YTDBClassSelect && isSameClassName(left, right) -> {
                ensureInvariants(left, right)
                val newCondition = left.condition.andNot(right.condition)
                val newOrder = left.order.merge(right.order)
                YTDBClassSelect(left.className, newCondition, newOrder)
            }

            else -> {
                YTDBDifferenceSelect(left, right)
            }
        }
    }

    fun distinct(source: YTDBSelect): YTDBSelect {
        return YTDBDistinctSelect(source)
    }

    fun reverse(query: YTDBSelect): YTDBSelect {
        val order = query.order?.reverse() ?: return query
        return query.withOrder(order)
    }

    private fun ensureInvariants(left: YTDBClassSelect, right: YTDBClassSelect) {
        ensureSkipIsNotUsed(left, right)
        ensureLimitIsNotUsed(left, right)
    }

    private fun ensureSkipIsNotUsed(left: YTDBSelect, right: YTDBSelect) {
        val lazyMessage = { "Skip can not be used for sub-query" }
        check(left.skip == null, lazyMessage)
        check(right.skip == null, lazyMessage)
    }

    private fun ensureLimitIsNotUsed(left: YTDBSelect, right: YTDBSelect) {
        val lazyMessage = { "Take can not be used for sub-query" }
        check(left.limit == null, lazyMessage)
        check(right.limit == null, lazyMessage)
    }

    private fun isSameClassName(left: YTDBClassSelect, right: YTDBClassSelect): Boolean {
        return left.className == right.className
    }
}

class YTDBCountSelect(
    val source: YTDBSelect,
) : YTDBQuery {

    override fun sql(builder: SqlBuilder) {
        builder.append("SELECT count(*) as count FROM (")
        source.sql(builder)
        builder.append(")")
    }

    fun count(tx: YTDBStoreTransaction): Long = YTDBQueryExecution.execute(this, tx).next().getProperty("count")
}

class YTDBFirstSelect(
    val source: YTDBSelect,
) : YTDBQuery {

    override fun sql(builder: SqlBuilder) {
        val index = builder.nextVarIndex()
        builder.append("SELECT expand(first(\$a${index})) LET \$a${index} = (")
        source.sql(builder)
        builder.append(")")
    }
}


class YTDBLastSelect(
    val source: YTDBSelect,
) : YTDBQuery {

    override fun sql(builder: SqlBuilder) {
        val index = builder.nextVarIndex()
        builder.append("SELECT expand(last(\$a${index})) LET \$a${index} = (")
        source.sql(builder)
        builder.append(")")
    }
}
