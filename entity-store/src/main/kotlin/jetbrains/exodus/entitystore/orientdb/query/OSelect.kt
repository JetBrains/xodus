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

import com.jetbrains.youtrack.db.api.record.RID

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

abstract class OSelectStub(
    override var order: OOrder? = null,
    override var skip: OSkip? = null,
    override var limit: OLimit? = null
) : OSelect {

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

abstract class OSelectBase(
    override var order: OOrder? = null,
    override var skip: OSkip? = null,
    override var limit: OLimit? = null
) : OSelectStub(order, skip, limit) {

    override fun sql(builder: SqlBuilder) {
        selectSql(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }

    abstract fun selectSql(builder: SqlBuilder)
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
}

class OLinkInFromIdsSelect(
    val linkName: String,
    val targetIds: List<RID>,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(in('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
    }

    private val targetIdsSql get() = "[${targetIds.map(RID::toString).joinToString(", ")}]"
}


class OLinkOfTypeInFromIdsSelect(
    val linkName: String,
    val targetIds: List<RID>,
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

    private val targetIdsSql get() = "[${targetIds.map(RID::toString).joinToString(", ")}]"
}

class OLinkOutFromIdSelect(
    val linkName: String,
    private val targetIds: List<RID>,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(out('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
    }

    private val targetIdsSql get() = "[${targetIds.map(RID::toString).joinToString(", ")}]"
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
        subQuery.sql(builder)
        builder.append(")")
    }
}


abstract class OBinaryOperationSelect(
    val left: OSelect,
    val right: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectStub(order, skip, limit) {

    abstract fun applyOperatorNoOrder(builder: SqlBuilder, leftArgName: String, rightArgName: String)
    open fun applyOperator(builder: SqlBuilder, leftArgName: String, rightArgName: String) = applyOperatorNoOrder(builder, leftArgName, rightArgName)

    open fun selectExpression(builder: SqlBuilder, argument:String) : SqlBuilder{
        return builder.append("SELECT expand($argument) ")
    }

    override fun sql(builder: SqlBuilder) {
        if (order == null || order is EmptyOrder) {
            // default action
            val index = builder.nextVarIndex()
            builder.append("SELECT expand(")

            applyOperatorNoOrder(builder, "\$a${index}", "\$b${index}")

            builder.append(") LET \$a${index}=(")
            left.sql(builder)
            builder.append("), \$b${index}=(")
            right.sql(builder)
            builder.append(")")

            skip.skip(builder)
            limit.limit(builder)
            order.orderBy(builder)
        } else {
/*
  SELECT expand($orderedResult)
  LET
  $a0 = (SELECT FROM GuestUser),
  $b0 = (SELECT FROM User),
  $unionResult = OPERATOR($a0, $b0),
  $orderedResult = (SELECT FROM $unionResult ORDER BY name)
 */
            val index = builder.nextVarIndex()
            selectExpression(builder, "\$orderedResult$index")
            builder.append("LET ")

            builder.append("\$a$index=(")
            left.sql(builder)
            builder.append("),")

            builder.append("\$b$index=(")
            right.sql(builder)
            builder.append("),")

            builder.append("\$operatorResult$index=")
            applyOperator(builder, "\$a${index}", "\$b${index}")
            builder.append(",")

            builder.append("\$orderedResult$index=(SELECT FROM \$operatorResult$index ")
            order.orderBy(builder)
            builder.append(")")
            skip.skip(builder)
            limit.limit(builder)
        }
    }
}

class OIntersectSelect(
    left: OSelect,
    right: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OBinaryOperationSelect(left, right, order, skip, limit) {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#intersect
    // intersect returns projection thus needs to expand it into collection
    override fun applyOperatorNoOrder(builder: SqlBuilder, leftArgName: String, rightArgName: String) {
        builder.append("intersect($leftArgName, $rightArgName)")
    }
}

class OUnionSelect(
    left: OSelect,
    right: OSelect,
    val distinct: Boolean = true,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OBinaryOperationSelect(left, right, order, skip, limit) {
    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#unionall
    // intersect returns projection thus needs to expand it into collection

    override fun selectExpression(builder: SqlBuilder, argument: String): SqlBuilder {
        return if (distinct) {
            super.selectExpression(builder, argument).append(".asSet() ")
        } else {
            super.selectExpression(builder, argument)
        }
    }

    override fun applyOperatorNoOrder(builder: SqlBuilder, leftArgName: String, rightArgName: String) {
        builder.append("unionall($leftArgName, $rightArgName)").apply {
            if (distinct){
                builder.append(".asSet()")
            }
        }
    }

    override fun applyOperator(builder: SqlBuilder, leftArgName: String, rightArgName: String) {
        builder.append("unionall($leftArgName, $rightArgName)")
    }
}

class ODistinctSelect(
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT DISTINCT * FROM (")
        subQuery.sql(builder)
        builder.append(")")
    }
}

class ODifferenceSelect(
    left: OSelect,
    right: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OBinaryOperationSelect(left, right, order, skip, limit) {

    override fun applyOperatorNoOrder(builder: SqlBuilder, leftArgName: String, rightArgName: String) {
        builder.append("difference($leftArgName, $rightArgName)")
    }
}

class ORecordIdSelect(
    val recordIds: Collection<RID>,
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
    if (this != null && this != EmptyOrder) {
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
