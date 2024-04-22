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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument

object OQueryFunctions {

    fun intersect(left: OSelect, right: OSelect): OSelect {
        return when {
            left is OClassSelect && right is OClassSelect -> {
                ensureSameClassName(left, right)
                val newCondition = left.condition.and(right.condition)
                val newOrder = left.order.merge(right.order)

                // narrow down the result set
                val newSkip = left.skip.max(right.skip)
                val newLimit = left.limit.min(right.limit)

                OClassSelect(left.className, newCondition, newOrder, newSkip, newLimit)
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
                val newCondition = left.condition.or(right.condition)
                val newOrder = left.order.merge(right.order)

                // shrink the result set
                val newSkip = left.skip.min(right.skip)
                val newLimit = left.limit.max(right.limit)

                OClassSelect(left.className, newCondition, newOrder, newSkip, newLimit)
            }

            else -> {
                OUnionSelect(left, right)
            }
        }
    }

    fun difference(left: OSelect, right: OSelect): OSelect {
        return ODifferenceSelect(left, right)
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

    override fun sql() = "SELECT count(*) as count FROM (${source.sql()})"
    override fun params() = source.params()

    fun count(session: ODatabaseDocument): Long = execute(session).next().getProperty<Long>("count")
}

class OFirstSelect(
    val source: OSelect,
) : OQuery {

    override fun sql() = "SELECT expand(first(*)) FROM (${source.sql()})"
    override fun params() = source.params()
}


class OLastSelect(
    val source: OSelect,
) : OQuery {

    override fun sql() = "SELECT expand(last(*)) FROM (${source.sql()})"
    override fun params() = source.params()
}