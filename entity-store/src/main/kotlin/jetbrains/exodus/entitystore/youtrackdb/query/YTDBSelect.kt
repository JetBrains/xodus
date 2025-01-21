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

import com.jetbrains.youtrack.db.api.record.RID

interface OConditional {
    val condition: YTDBCondition?
}

interface OSortable {
    val order: YTDBOrder?

    fun withOrder(order: YTDBOrder): YTDBSelect
}

interface OSizable {
    val skip: YTDBSkip?
    val limit: YTDBLimit?

    fun withSkip(skip: Int): YTDBSelect
    fun withLimit(limit: Int): YTDBSelect
}

sealed interface YTDBSelect : YTDBQuery, OSortable, OSizable

abstract class YTDBSelectStub(
    override var order: YTDBOrder? = null,
    override var skip: YTDBSkip? = null,
    override var limit: YTDBLimit? = null
) : YTDBSelect {

    override fun withOrder(order: YTDBOrder): YTDBSelect {
        this.order = this.order?.merge(order) ?: order
        return this
    }

    override fun withSkip(skip: Int): YTDBSelect {
        this.skip = YTDBSkipValue(skip)
        return this
    }

    override fun withLimit(limit: Int): YTDBSelect {
        this.limit = YTDBLimitValue(limit)
        return this
    }
}

abstract class YTDBSelectBase(
    override var order: YTDBOrder? = null,
    override var skip: YTDBSkip? = null,
    override var limit: YTDBLimit? = null
) : YTDBSelectStub(order, skip, limit) {

    override fun sql(builder: SqlBuilder) {
        selectSql(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }

    abstract fun selectSql(builder: SqlBuilder)
}


class YTDBClassSelect(
    val className: String,
    override val condition: YTDBCondition? = null,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(order, skip, limit), OConditional {

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

class YTDBLinkInFromSubQuerySelect(
    val linkName: String,
    val subQuery: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(in('").append(linkName).append("')) FROM (")
        subQuery.sql(builder)
        builder.append(")")
    }
}

class YTDBLinkInFromIdsSelect(
    val linkName: String,
    val targetIds: List<RID>,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(in('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
    }

    private val targetIdsSql get() = "[${targetIds.map(RID::toString).joinToString(", ")}]"
}


class YTDBLinkOfTypeInFromIdsSelect(
    val linkName: String,
    val targetIds: List<RID>,
    val targetEntityType: String,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder
            .append("SELECT FROM (")
            .append("SELECT expand(in('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
            .append(") WHERE @class='").append(targetEntityType).append("'")

    }

    private val targetIdsSql get() = "[${targetIds.map(RID::toString).joinToString(", ")}]"
}

class YTDBLinkOutFromIdSelect(
    val linkName: String,
    private val targetIds: List<RID>,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(out('").append(linkName).append("')) FROM ")
            .append(targetIdsSql)
    }

    private val targetIdsSql get() = "[${targetIds.map(RID::toString).joinToString(", ")}]"
}



class YTDBLinkOutFromSubQuerySelect(
    val linkName: String,
    val subQuery: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT expand(out('").append(linkName).append("')) FROM (")
        subQuery.sql(builder)
        builder.append(")")
    }
}


abstract class YTDBBinaryOperationSelect(
    val left: YTDBSelect,
    val right: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectStub(order, skip, limit) {

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

class YTDBIntersectSelect(
    left: YTDBSelect,
    right: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBBinaryOperationSelect(left, right, order, skip, limit) {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#intersect
    // intersect returns projection thus needs to expand it into collection
    override fun applyOperatorNoOrder(builder: SqlBuilder, leftArgName: String, rightArgName: String) {
        builder.append("intersect($leftArgName, $rightArgName)")
    }
}

class YTDBUnionSelect(
    left: YTDBSelect,
    right: YTDBSelect,
    val distinct: Boolean = true,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBBinaryOperationSelect(left, right, order, skip, limit) {
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

class YTDBDistinctSelect(
    val subQuery: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(order, skip, limit) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT DISTINCT * FROM (")
        subQuery.sql(builder)
        builder.append(")")
    }
}

class YTDBDifferenceSelect(
    left: YTDBSelect,
    right: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBBinaryOperationSelect(left, right, order, skip, limit) {

    override fun applyOperatorNoOrder(builder: SqlBuilder, leftArgName: String, rightArgName: String) {
        builder.append("difference($leftArgName, $rightArgName)")
    }
}

class YTDBRecordIdSelect(
    val recordIds: Collection<RID>,
    order: YTDBOrder? = null
) : YTDBSelectBase(order) {

    override fun selectSql(builder: SqlBuilder) {
        builder.append("SELECT FROM ")
            .append("[")
            .append(recordIds.joinToString(", ") { it.toString() })
            .append("]")
    }
}

fun YTDBCondition?.where(builder: SqlBuilder) {
    this?.let {
        builder.append(" WHERE ")
        it.sql(builder)
    } ?: builder.append("")
}

fun YTDBOrder?.orderBy(builder: SqlBuilder) {
    if (this != null && this != EmptyOrder) {
        builder.append(" ORDER BY ")
        this.sql(builder)
    }
}

fun YTDBSkip?.skip(builder: SqlBuilder) {
    this?.let {
        builder.append(" SKIP ")
        it.sql(builder)
    } ?: builder.append("")
}

fun YTDBLimit?.limit(builder: SqlBuilder) {
    this?.let {
        builder.append(" LIMIT ")
        it.sql(builder)
    } ?: builder.append("")
}
