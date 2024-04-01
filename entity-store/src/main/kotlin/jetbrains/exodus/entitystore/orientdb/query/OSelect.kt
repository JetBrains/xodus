package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.orient.core.id.ORID

sealed interface OSelect : OQuery {
    val condition: OCondition?
    val order: OOrder?

    fun withOrder(field: String, ascending: Boolean): OSelect
}

class OClassSelect(
    val className: String,
    override val condition: OCondition? = null,
    override val order: OOrder? = null
) : OSelect {

    override fun sql() = "SELECT FROM $className" +
            condition.where() +
            order.orderBy()

    override fun params() = condition?.params() ?: emptyList()

    override fun withOrder(field: String, ascending: Boolean): OClassSelect {
        return OClassSelect(className, condition, OOrderByField(field, ascending))
    }
}


class OLinkInFromSubQuerySelect(
    val linkName: String,
    val subQuery: OQuery,
    override val condition: OCondition? = null,
    override val order: OOrder? = null
) : OSelect {

    override fun sql() = "SELECT expand(in('$linkName')) FROM (${subQuery.sql()})" +
            condition.where() +
            order.orderBy()

    override fun params() = subQuery.params() + condition?.params().orEmpty()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, condition, OOrderByField(field, ascending))
    }
}

class OLinkInFromIdsSelect(
    val linkName: String,
    val targetIds: List<ORID>,
    override val condition: OCondition? = null,
    override val order: OOrder? = null
) : OSelect {

    override fun sql() = "SELECT expand(in('$linkName')) FROM $targetIdsSql" +
            condition.where() +
            order.orderBy()

    private val targetIdsSql get() = "[${targetIds.map(ORID::toString).joinToString(", ")}]"

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromIdsSelect(linkName, targetIds, condition, OOrderByField(field, ascending))
    }
}

class OIntersectSelect(
    val left: OSelect,
    val right: OSelect,
    override val order: OOrder? = null
) : OSelect {

    override val condition: OCondition? = null

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#intersect
    // intersect returns projection thus need to expand it into collection
    override fun sql() = "SELECT expand(intersect((${left.sql()}), (${right.sql()})))" + order.orderBy()
    override fun params() = left.params() + right.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OIntersectSelect(left, right, OOrderByField(field, ascending))
    }
}

class OUnionSelect(
    val left: OSelect,
    val right: OSelect,
    override val order: OOrder? = null
) : OSelect {

    override val condition: OCondition? = null

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#unionall
    // intersect returns projection thus need to expand it into collection
    override fun sql() = "SELECT expand(unionall((${left.sql()}), (${right.sql()})))" + order.orderBy()
    override fun params() = left.params() + right.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OUnionSelect(left, right, OOrderByField(field, ascending))
    }
}

class OCountSelect(
    val source: OSelect,
) : OSelect {

    override val order: OOrder? = null
    override val condition: OCondition? = null

    override fun sql() = "SELECT count(*) as count FROM (${source.sql()})"
    override fun params() = source.params()

    override fun withOrder(field: String, ascending: Boolean) = this

    fun count(): Long = execute().next().getProperty<Long>("count")
}

class ODistinctSelect(
    val source: OSelect,
    override val order: OOrder? = null,
    override val condition: OCondition? = null
) : OSelect {

    override fun sql() = "SELECT DISTINCT FROM (${source.sql()})" + order.orderBy()
    override fun params() = source.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return ODistinctSelect(source, OOrderByField(field, ascending), condition)
    }
}

class ODifferenceSelect(
    val left: OSelect,
    val right: OSelect,
    override val order: OOrder? = null,
    override val condition: OCondition? = null
) : OSelect {

    override fun sql() = "SELECT expand(difference((${left.sql()}), (${right.sql()})))" + order.orderBy()
    override fun params() = left.params() + right.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return ODifferenceSelect(left, right, OOrderByField(field, ascending), condition)
    }
}

fun OCondition?.where() = this?.let { " WHERE ${it.sql()}" } ?: ""
fun OOrder?.orderBy() = this?.let { " ORDER BY ${it.sql()}" } ?: ""