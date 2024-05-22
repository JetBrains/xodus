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

import com.orientechnologies.orient.core.id.ORID

interface OConditional {
    val condition: OCondition?
}

interface OSortable {
    val order: OOrder?

    fun withOrder(field: String, ascending: Boolean): OSelect
}

interface OSizable {
    val skip: OSkip?
    val limit: OLimit?

    fun withSkip(skip: Int): OSelect
    fun withLimit(limit: Int): OSelect
}

sealed interface OSelect : OQuery, OSortable, OSizable

abstract class OSelectBase(
    override var order: OOrder? = null,
    override var skip: OSkip? = null,
    override var limit: OLimit? = null
) : OSelect {

    override fun sql(builder: StringBuilder) {
        selectSql(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }

    abstract fun selectSql(builder: StringBuilder)

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        val newOrder = OOrderByFields(field, ascending)
        order = order?.merge(newOrder) ?: newOrder
        return this
    }

    override fun withSkip(skipValue: Int): OSelect {
        skip = OSkipValue(skipValue)
        return this
    }

    override fun withLimit(limitValue: Int): OSelect {
        limit = OLimitValue(limitValue)
        return this
    }
}


class OClassSelect(
    val className: String,
    override val condition: OCondition? = null,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit), OConditional {

    override fun sql(builder: StringBuilder) {
        selectSql(builder)
        condition.where(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }

    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT FROM ")
        builder.append(className)
    }

    override fun params() = condition?.params() ?: emptyList()
}

class OLinkInFromSubQuerySelect(
    val linkName: String,
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT expand(in('").append(linkName).append("')) FROM (")
        subQuery.sql(builder)
        builder.append(")")
    }

    override fun params() = subQuery.params()
}

class OLinkInFromIdsSelect(
    val linkName: String,
    val targetIds: List<ORID>,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT expand(in('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
    }

    private val targetIdsSql get() = "[${targetIds.map(ORID::toString).joinToString(", ")}]"
}

class OLinkOutFromSubQuerySelect(
    val linkName: String,
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT expand(out('").append(linkName).append("')) FROM (")
        if (subQuery is OConditional && subQuery.condition != null){
            val linkExistsCondition = OAndCondition(subQuery.condition!!, OEdgeExistsCondition(linkName))
            (subQuery as OSelectBase).selectSql(builder)
            linkExistsCondition.where(builder)
            subQuery.order.orderBy(builder)
            subQuery.skip.skip(builder)
            subQuery.limit.limit(builder)
        } else {
            subQuery.sql(builder)
            OEdgeExistsCondition(linkName).where(builder)
        }
        builder.append(")")
    }

    override fun params() = subQuery.params()
}

class OIntersectSelect(
    val left: OSelect,
    val right: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#intersect
    // intersect returns projection thus need to expand it into collection
    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT expand(intersect(\$a, \$b)) LET \$a=(")
        left.sql(builder)
        builder.append("), \$b=(")
        right.sql(builder)
        builder.append(")")
    }


    override fun params() = left.params() + right.params()
}

class OUnionSelect(
    val left: OSelect,
    val right: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#unionall
    // intersect returns projection thus need to expand it into collection
    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT expand(unionall(\$a, \$b)) LET \$a=(")
        left.sql(builder)
        builder.append("), \$b=(")
        right.sql(builder)
        builder.append(")")
    }

    override fun params() = left.params() + right.params()
}

class ODistinctSelect(
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT DISTINCT * FROM (")
        subQuery.sql(builder)
        builder.append(")")
    }

    override fun params() = subQuery.params()
}

class ODifferenceSelect(
    val left: OSelect,
    val right: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT expand(difference(\$a, \$b)) LET \$a=(")
        left.sql(builder)
        builder.append("), \$b=(")
        right.sql(builder)
        builder.append(")")
    }

    override fun params() = left.params() + right.params()
}

class OSingleSelect(private val orid: ORID) : OSelectBase(){

    override fun selectSql(builder: StringBuilder) {
        builder.append("SELECT FROM ").append(orid)
    }
}

fun OCondition?.where(builder: StringBuilder) {
    this?.let {
        builder.append(" WHERE ")
        it.sql(builder)
    } ?: builder.append("")
}

fun OOrder?.orderBy(builder: StringBuilder) {
    this?.let {
        builder.append(" ORDER BY ")
        it.sql(builder)
    } ?: builder.append("")
}

fun OSkip?.skip(builder: StringBuilder) {
    this?.let {
        builder.append(" SKIP ")
        it.sql(builder)
    } ?: builder.append("")
}

fun OLimit?.limit(builder: StringBuilder) {
    this?.let {
        builder.append(" LIMIT ")
        it.sql(builder)
    } ?: builder.append("")
}
