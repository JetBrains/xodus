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

    override fun sql() = selectSql() + order.orderBy() + skip.skip() + limit.limit()
    abstract fun selectSql(): String

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

    override fun sql() = selectSql() + condition.where() + order.orderBy() + skip.skip() + limit.limit()

    override fun selectSql() = "SELECT FROM $className"

    override fun params() = condition?.params() ?: emptyList()
}

class OLinkInFromSubQuerySelect(
    val linkName: String,
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql() = "SELECT expand(in('$linkName')) FROM (${subQuery.sql()})"

    override fun params() = subQuery.params()
}

class OLinkInFromIdsSelect(
    val linkName: String,
    val targetIds: List<ORID>,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql() = "SELECT expand(in('$linkName')) FROM $targetIdsSql"

    private val targetIdsSql get() = "[${targetIds.map(ORID::toString).joinToString(", ")}]"
}

class OLinkOutFromSubQuerySelect(
    val linkName: String,
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql() = "SELECT expand(out('$linkName')) FROM (${subQuery.sql()})"

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
    override fun selectSql() = "SELECT expand(intersect(\$a, \$b)) LET \$a=(${left.sql()}), \$b=(${right.sql()})"

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
    override fun selectSql() = "SELECT expand(unionall(\$a, \$b)) LET \$a=(${left.sql()}), \$b=(${right.sql()})"

    override fun params() = left.params() + right.params()
}

class ODistinctSelect(
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql() = "SELECT DISTINCT * FROM (${subQuery.sql()})"

    override fun params() = subQuery.params()
}

class ODifferenceSelect(
    val left: OSelect,
    val right: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql() = "SELECT expand(difference(\$a, \$b)) LET \$a=(${left.sql()}), \$b=(${right.sql()})"

    override fun params() = left.params() + right.params()
}

fun OCondition?.where() = this?.let { " WHERE ${it.sql()}" } ?: ""
fun OOrder?.orderBy() = this?.let { " ORDER BY ${it.sql()}" } ?: ""
fun OSkip?.skip() = this?.let { " SKIP ${it.sql()}" } ?: ""
fun OLimit?.limit() = this?.let { " LIMIT ${it.sql()}" } ?: ""