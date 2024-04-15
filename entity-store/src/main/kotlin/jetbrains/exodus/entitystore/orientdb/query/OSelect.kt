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
    override val order: OOrder? = null,
    override val skip: OSkip? = null,
    override val limit: OLimit? = null
) : OSelect {

    override fun sql() = selectSql() + order.orderBy() + skip.skip() + limit.limit()
    abstract fun selectSql(): String
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

    override fun withOrder(field: String, ascending: Boolean): OClassSelect {
        return OClassSelect(className, condition, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OClassSelect {
        return OClassSelect(className, condition, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OClassSelect {
        return OClassSelect(className, condition, order, OSkipValue(skip), limit)
    }
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

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, order, OSkipValue(skip), limit)
    }
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

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromIdsSelect(linkName, targetIds, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OSelect {
        return OLinkInFromIdsSelect(linkName, targetIds, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OSelect {
        return OLinkInFromIdsSelect(linkName, targetIds, order, OSkipValue(skip), limit)
    }
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

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, order, OSkipValue(skip), limit)
    }
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

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OIntersectSelect(left, right, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OSelect {
        return OIntersectSelect(left, right, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OSelect {
        return OIntersectSelect(left, right, order, OSkipValue(skip), limit)
    }
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

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OUnionSelect(left, right, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OSelect {
        return OUnionSelect(left, right, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OSelect {
        return OUnionSelect(left, right, order, OSkipValue(skip), limit)
    }
}

class ODistinctSelect(
    val subQuery: OSelect,
    order: OOrder? = null,
    skip: OSkip? = null,
    limit: OLimit? = null
) : OSelectBase(order, skip, limit) {

    override fun selectSql() = "SELECT DISTINCT * FROM (${subQuery.sql()})"

    override fun params() = subQuery.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return ODistinctSelect(subQuery, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OSelect {
        return ODistinctSelect(subQuery, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OSelect {
        return ODistinctSelect(subQuery, order, OSkipValue(skip), limit)
    }
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

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return ODifferenceSelect(left, right, OOrderByField(field, ascending))
    }

    override fun withLimit(limit: Int): OSelect {
        return ODifferenceSelect(left, right, order, skip, OLimitValue(limit))
    }

    override fun withSkip(skip: Int): OSelect {
        return ODifferenceSelect(left, right, order, OSkipValue(skip), limit)
    }
}

fun OCondition?.where() = this?.let { " WHERE ${it.sql()}" } ?: ""
fun OOrder?.orderBy() = this?.let { " ORDER BY ${it.sql()}" } ?: ""
fun OSkip?.skip() = this?.let { " SKIP ${it.sql()}" } ?: ""
fun OLimit?.limit() = this?.let { " LIMIT ${it.sql()}" } ?: ""