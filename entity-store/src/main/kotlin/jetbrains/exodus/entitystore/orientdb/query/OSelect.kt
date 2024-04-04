package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.id.ORID

interface OConditional {
    val condition: OCondition?
}

interface OSortable {
    val order: OOrder?
    fun withOrder(field: String, ascending: Boolean): OSelect
}

sealed interface OSelect : OQuery, OSortable

class OClassSelect(
    val className: String,
    override val condition: OCondition? = null,
    override val order: OOrder? = null
) : OSelect, OConditional {

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
    val subQuery: OSelect,
    override val order: OOrder? = null
) : OSelect {

    override fun sql() = "SELECT expand(in('$linkName')) FROM (${subQuery.sql()})" + order.orderBy()

    override fun params() = subQuery.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, OOrderByField(field, ascending))
    }
}

class OLinkInFromIdsSelect(
    val linkName: String,
    val targetIds: List<ORID>,
    override val order: OOrder? = null
) : OSelect {

    override fun sql() = "SELECT expand(in('$linkName')) FROM $targetIdsSql" + order.orderBy()

    private val targetIdsSql get() = "[${targetIds.map(ORID::toString).joinToString(", ")}]"

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromIdsSelect(linkName, targetIds, OOrderByField(field, ascending))
    }
}

class OLinkOutFromSubQuerySelect(
    val linkName: String,
    val subQuery: OSelect,
    override val order: OOrder? = null
) : OSelect {

    override fun sql() = "SELECT expand(out('$linkName')) FROM (${subQuery.sql()})" + order.orderBy()

    override fun params() = subQuery.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return OLinkInFromSubQuerySelect(linkName, subQuery, OOrderByField(field, ascending))
    }
}

class OIntersectSelect(
    val left: OSelect,
    val right: OSelect,
    override val order: OOrder? = null
) : OSelect {

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

    override fun sql() = "SELECT count(*) as count FROM (${source.sql()})"
    override fun params() = source.params()

    override fun withOrder(field: String, ascending: Boolean) = this

    fun count(session: ODatabaseDocument? = null): Long = execute(session).next().getProperty<Long>("count")
}

class ODistinctSelect(
    val subQuery: OSelect,
    override val order: OOrder? = null,
) : OSelect {

    override fun sql() = "SELECT distinct(${subQuery.sql()})" + order.orderBy()
    override fun params() = subQuery.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return ODistinctSelect(subQuery, OOrderByField(field, ascending))
    }
}

class ODifferenceSelect(
    val left: OSelect,
    val right: OSelect,
    override val order: OOrder? = null,
) : OSelect {

    override fun sql() = "SELECT expand(difference((${left.sql()}), (${right.sql()})))" + order.orderBy()
    override fun params() = left.params() + right.params()

    override fun withOrder(field: String, ascending: Boolean): OSelect {
        return ODifferenceSelect(left, right, OOrderByField(field, ascending))
    }
}

fun OCondition?.where() = this?.let { " WHERE ${it.sql()}" } ?: ""
fun OOrder?.orderBy() = this?.let { " ORDER BY ${it.sql()}" } ?: ""