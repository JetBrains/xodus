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

    fun hasPagination(): Boolean = skip != null || limit != null
}

sealed interface YTDBSelect : YTDBQuery, OSortable, OSizable {

    fun nest(select: String, canCount: Boolean = false) = YTDBSelectNested(
        nested = this,
        canCount = canCount,
        projection = { append(select) }
    )

    fun selectInLinks(linkName: String) =
        if (this is YTDBClassSelect && !hasPagination())
            select("expand(in('$linkName'))", ignoreOrder = true, canCount = false)
        else nest("expand(in('$linkName'))")

    fun selectOutLinks(linkName: String) =
        if (this is YTDBClassSelect && !hasPagination())
            select("expand(out('$linkName'))", ignoreOrder = true, canCount = false)
        else nest("expand(out('$linkName'))")
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
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null,
    val canCount: Boolean = false,
    val list: SqlBuilder.() -> SqlBuilder,
    val body: SqlBuilder.() -> SqlBuilder,
) : YTDBSelectStub(order, skip, limit) {

    fun select(
        projection: String,
        canCount: Boolean? = null,
        ignoreOrder: Boolean = false,
        ignorePagination: Boolean = false,
    ) = YTDBSelectProjection(
        delegate = this,
        order = if (ignoreOrder) null else order,
        skip = if (ignorePagination) null else skip,
        limit = if (ignorePagination) null else limit,
        canCount = canCount ?: this.canCount,
        projection = { append(projection) }
    )

    final override fun sql(builder: SqlBuilder) {
        builder.append("SELECT").ensureWhitespace()
        list(builder).ensureWhitespace()
        body(builder)
        order.orderBy(builder)
        skip.skip(builder)
        limit.limit(builder)
    }
}

class YTDBSelectNested(
    nested: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null,
    canCount: Boolean = false,
    projection: SqlBuilder.() -> SqlBuilder,
) : YTDBSelectBase(
    order = order,
    skip = skip,
    limit = limit,
    canCount = canCount,
    list = projection,
    body = { append("FROM (").appendSql(nested).append(")") }
)

class YTDBSelectProjection(
    delegate: YTDBSelectBase,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null,
    canCount: Boolean = false,
    projection: SqlBuilder.() -> SqlBuilder,
) : YTDBSelectBase(
    order = order,
    skip = skip,
    limit = limit,
    canCount = canCount,
    list = projection,
    body = { delegate.body(this) }
)

class YTDBClassSelect(
    val className: String,
    override val condition: YTDBCondition? = null,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(
    order = order,
    skip = skip,
    limit = limit,
    canCount = true,
    list = { this },
    body = { append("FROM ").append(className).where(condition) }
), OConditional

class YTDBLinkInFromIdsSelect(
    linkName: String,
    targetIds: List<RID>,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(
    order = order,
    skip = skip,
    limit = limit,
    canCount = true, // todo: check this
    list = { append("expand(in('").append(linkName).append("'))") },
    body = { append("FROM ").param("targetIds", targetIds) }
)

class YTDBLinkOfTypeInFromIdsSelect(
    linkName: String,
    targetIds: List<RID>,
    targetEntityType: String,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(
    order = order,
    skip = skip,
    limit = limit,
    canCount = true,
    list = { this },
    body = {
        append("FROM")
            .append(" (SELECT expand(in('").append(linkName).append("'))")
            .append(" FROM ").param("targetIds", targetIds)
            .append(")")
            .append(" WHERE @class='$targetEntityType'")
    }
) 

class YTDBLinkOutFromIdSelect(
    linkName: String,
    targetIds: List<RID>,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectBase(
    order = order,
    skip = skip,
    limit = limit,
    canCount = true, // todo: check this
    list = { append("expand(out('").append(linkName).append("'))") },
    body = { append("FROM ").param("targetIds", targetIds) }
) 

abstract class YTDBBinaryOperationSelect(
    val left: YTDBSelect,
    val right: YTDBSelect,
    order: YTDBOrder? = null,
    skip: YTDBSkip? = null,
    limit: YTDBLimit? = null
) : YTDBSelectStub(order, skip, limit) {

    abstract fun applyOperatorNoOrder(builder: SqlBuilder, leftArgName: String, rightArgName: String)
    open fun applyOperator(builder: SqlBuilder, leftArgName: String, rightArgName: String) =
        applyOperatorNoOrder(builder, leftArgName, rightArgName)

    open fun selectExpression(builder: SqlBuilder, argument: String): SqlBuilder =
        builder.append("SELECT expand($argument) ")

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
    order = order,
    canCount = true,
    list = { this },
    body = { append("FROM ").param("rids", recordIds) }
) 


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
