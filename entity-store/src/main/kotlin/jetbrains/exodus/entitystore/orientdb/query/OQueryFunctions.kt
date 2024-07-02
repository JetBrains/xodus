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
            left is OClassSelect && right is OClassSelect -> {
                ensureSameClassName(left, right)
                check(left.skip == null && right.skip == null) { "Skip can not be used for sub-query when intersect" }
                check(left.limit == null && right.limit == null) { "Take can not be used for sub-query when intersect" }

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
            left is OClassSelect && right is OClassSelect -> {
                ensureSameClassName(left, right)
                check(left.skip == null && right.skip == null) { "Skip can not be used for sub-query when union" }
                check(left.limit == null && right.limit == null) { "Take can not be used for sub-query when union" }

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
            left is OClassSelect && right is OClassSelect -> {
                ensureSameClassName(left, right)
                check(left.skip == null && right.skip == null) { "Skip can not be used for sub-query when minus" }
                check(left.limit == null && right.limit == null) { "Take can not be used for sub-query when minus" }

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

    private fun ensureSameClassName(left: OClassSelect, right: OClassSelect) {
        require(left.className == right.className) { "Cannot intersect different DB classes: ${left.className} and ${right.className}" }
    }
}

class OCountSelect(
    val source: OSelect,
) : OQuery {

    override fun sql(builder: SqlBuilder) {
        builder.append("SELECT count(*) as count FROM (")
        source.sql(builder.deepen())
        builder.append(")")
    }

    override fun params() = source.params()

    fun count(tx: OStoreTransaction): Long = OQueryExecution.execute(this, tx).next().getProperty("count")
}

class OFirstSelect(
    val source: OSelect,
) : OQuery {

    override fun sql(builder: SqlBuilder) {
        val depth = builder.depth
        builder.append("SELECT expand(first(\$a${depth})) LET \$a${depth} = (")
        source.sql(builder.deepen())
        builder.append(")")
    }

    override fun params() = source.params()
}


class OLastSelect(
    val source: OSelect,
) : OQuery {

    override fun sql(builder: SqlBuilder) {
        val depth = builder.depth
        builder.append("SELECT expand(last(\$a${depth})) LET \$a${depth} = (")
        source.sql(builder.deepen())
        builder.append(")")
    }

    override fun params() = source.params()
}