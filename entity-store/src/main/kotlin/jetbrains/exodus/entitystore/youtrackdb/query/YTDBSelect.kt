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

typealias SqlStep = (SqlBuilder.() -> SqlBuilder)

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

sealed interface YTDBSelect : YTDBQuery, OSortable, OSizable {
    fun count(): YTDBSelect =
        if (this is YTDBSelectBase && this.count != null)
            YTDBSimpleCountSelect(this, this.count)
        else YTDBNestedCountSelect(this)
}

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
    val list: SqlStep?,
    val count: SqlStep? = null,
    val from: SqlStep,
    val condition: YTDBCondition? = null,
    override var order: YTDBOrder? = null,
    override var skip: YTDBSkip? = null,
    override var limit: YTDBLimit? = null,
) : YTDBSelectStub(order, skip, limit) {

    final override fun sql(builder: SqlBuilder) {
        builder.append("SELECT ")
        list?.let { it(builder).append(" ") }

        builder.append("FROM ")
        from(builder)

        condition.where(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }
}

class YTDBSimpleCountSelect(
    source: YTDBSelectBase,
    countStep: SqlStep
) : YTDBSelectBase(
    list = { countStep(this).append(" AS count") },
    from = source.from,
    condition = source.condition,
    order = null,
    skip = source.skip,
    limit = source.limit,
)

class YTDBNestedCountSelect(
    val subQuery: YTDBSelect,
) : YTDBSelectBase(
    list = { append("COUNT(*) AS count") },
    from = { nested(subQuery) }
)

class YTDBClassSelect(
    val className: String,
    where: YTDBCondition? = null,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(
    list = null,
    count = { append("COUNT(*)") },
    from = { append(className) },
    condition = where,
    order = order,
    skip = skip,
    limit = limit,
)

class YTDBLinkInFromSubQuerySelect(
    linkName: String,
    subQuery: YTDBSelect,
) : YTDBSelectBase(
    list = { inLinks(linkName, expand = true) },
    count = { inLinks(linkName).append(".size()") },
    from = { nested(subQuery) }
)

class YTDBLinkInFromIdsSelect(
    linkName: String,
    targetIds: List<RID>
) : YTDBSelectBase(
    list = { inLinks(linkName, expand = true) },
    count = { inLinks(linkName).append(".size()") },
    from = { param("targetIds", targetIds) }
)

class YTDBLinkOfTypeInFromIdsSelect(
    linkName: String,
    targetIds: List<RID>,
    targetEntityType: String,
) : YTDBSelectBase(
    list = null,
    count = { append("COUNT(*)") },
    from = {
        append("(")
            .append("SELECT ").inLinks(linkName, expand = true)
            .append(" FROM ").param("targetIds", targetIds)
            .append(")")
            .append(" WHERE @class='$targetEntityType'")
    }
)

class YTDBLinkOutFromIdSelect(
    val linkName: String,
    val targetIds: List<RID>,
) : YTDBSelectBase(
    list = { outLinks(linkName, expand = true) },
    count = { outLinks(linkName).append(".size()") },
    from = { param("targetIds", targetIds) }
)

class YTDBLinkOutFromSubQuerySelect(
    linkName: String,
    subQuery: YTDBSelect,
) : YTDBSelectBase(
    list = { outLinks(linkName, expand = true) },
    count = { outLinks(linkName).append(".size()") },
    from = { nested(subQuery) }
)

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
) : YTDBSelectBase(
    list = { append("DISTINCT *") },
    from = { nested(subQuery) }
)

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
) : YTDBSelectBase(
    list = null,
    from = { param("rids", recordIds) },
    order = order,
)

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
