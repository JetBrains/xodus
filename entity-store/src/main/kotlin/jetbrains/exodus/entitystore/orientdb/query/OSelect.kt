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

    fun withOrder(order: OOrder): OSelect
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

    override fun sql(builder: SqlBuilder) {
        selectSql(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }

    abstract fun selectSql(builder: SqlBuilder)

    override fun withOrder(order: OOrder): OSelect {
        this.order = this.order?.merge(order) ?: order
        return this
    }

    override fun withSkip(skip: Int): OSelect {
        this.skip = OSkipValue(skip)
        return this
    }

    override fun withLimit(limit: Int): OSelect {
        this.limit = OLimitValue(limit)
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

    override fun sql(builder: SqlBuilder) {
        selectSql(builder)
        condition.where(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }

    override fun selectSql(builder: SqlBuilder) {
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

    override fun selectSql(builder: SqlBuilder) {
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

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(in('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
    }

    private val targetIdsSql get() = "[${targetIds.map(ORID::toString).joinToString(", ")}]"
}


class OLinkOfTypeInFromIdsSelect(
    val linkName: String,
    val targetIds: List<ORID>,
    val targetEntityType: String,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder
            .append("SELECT FROM (")
            .append("SELECT expand(in('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
            .append(") WHERE @class='").append(targetEntityType).append("'")

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

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(out('").append(linkName).append("')) FROM (")
        subQuery.sql(builder.deepen())
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
    override fun selectSql(builder: SqlBuilder) {
        val depth = builder.depth
        builder.append("SELECT expand(intersect(\$a${depth}, \$b${depth})) LET \$a${depth}=(")
        left.sql(builder.deepen())
        builder.append("), \$b${depth}=(")
        right.sql(builder.deepen())
        builder.append(")")
    }


    override fun params() = left.params() + right.params()
}

class OUnionSelect(
    val left: OSelect,
    val right: OSelect,
    val distinct: Boolean = true,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#unionall
    // intersect returns projection thus need to expand it into collection
    override fun selectSql(builder: SqlBuilder) {
        val depth = builder.depth
        builder.append("SELECT expand(unionall(\$a${depth}, \$b${depth})")
        if (distinct) builder.append(".asSet()")
        builder.append(") LET \$a${depth}=(")
        left.sql(builder.deepen())
        builder.append("), \$b${depth}=(")
        right.sql(builder.deepen())
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

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT DISTINCT * FROM (")
        subQuery.sql(builder.deepen())
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

    override fun selectSql(builder: SqlBuilder) {
        val depth = builder.depth
        builder.append("SELECT expand(difference(\$a${depth}, \$b${depth})) LET \$a${depth}=(")
        left.sql(builder.deepen())
        builder.append("), \$b${depth}=(")
        right.sql(builder.deepen())
        builder.append(")")
    }

    override fun params() = left.params() + right.params()
}

class ORecordIdSelect(
    val recordIds: Collection<ORID>,
    order: OOrder? = null
) : OSelectBase(order) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT FROM ")
            .append("[")
            .append(recordIds.joinToString(", ") { it.toString() })
            .append("]")
    }
}

fun OCondition?.where(builder: SqlBuilder) {
    this?.let {
        builder.append(" WHERE ")
        it.sql(builder)
    } ?: builder.append("")
}

fun OOrder?.orderBy(builder: SqlBuilder) {
    if (this != null && this != EmptyOrder){
        builder.append(" ORDER BY ")
        this.sql(builder)
    }
}

fun OSkip?.skip(builder: SqlBuilder) {
    this?.let {
        builder.append(" SKIP ")
        it.sql(builder)
    } ?: builder.append("")
}

fun OLimit?.limit(builder: SqlBuilder) {
    this?.let {
        builder.append(" LIMIT ")
        it.sql(builder)
    } ?: builder.append("")
}
