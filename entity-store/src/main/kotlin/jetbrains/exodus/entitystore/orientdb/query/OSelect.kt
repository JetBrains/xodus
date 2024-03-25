package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.orient.core.id.ORID

sealed interface OClassSelect : OQuery {
    val className: String
    val condition: OCondition?
    val order: OOrder?

    fun withOrder(field: String, ascending: Boolean): OClassSelect
}

class OAllSelect(
    override val className: String,
    override val condition: OCondition? = null,
    override val order: OOrder? = null
) : OClassSelect {

    override fun sql() = ("SELECT from $className" +
            " ${condition.whereOrEmpty()}" +
            " ${order.orderByOrEmpty()}").trimEnd()

    override fun params() = condition?.params() ?: emptyList()

    override fun withOrder(field: String, ascending: Boolean): OAllSelect {
        return OAllSelect(className, condition, OOrderByField(field, ascending))
    }
}

class OLinkInSelect(
    override val className: String,
    val linkName: String,
    val targetIds: List<ORID>,
    override val condition: OCondition? = null,
    override val order: OOrder? = null
) : OClassSelect {

    override fun sql() = ("SELECT expand(in('$linkName')) from $targetIdsSql" +
            " ${condition.whereOrEmpty()} " +
            " ${order.orderByOrEmpty()}").trimEnd()

    private val targetIdsSql get() = "[${targetIds.map(ORID::toString).joinToString(", ")}]"

    override fun withOrder(field: String, ascending: Boolean): OClassSelect {
        return OLinkInSelect(className, linkName, targetIds, condition, OOrderByField(field, ascending))
    }
}

class OIntersectSelect(
    override val className: String,
    val left: OClassSelect,
    val right: OClassSelect,
    override val order: OOrder? = null
) : OClassSelect {

    override val condition: OCondition? = null

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#intersect
    // intersect returns projection thus need to expand it into collection
    override fun sql() = "SELECT expand(intersect((${left.sql()}), (${right.sql()}))) ${order.orderByOrEmpty()}".trim()
    override fun params() = left.params() + right.params()

    override fun withOrder(field: String, ascending: Boolean): OClassSelect {
        return OIntersectSelect(className, left, right, OOrderByField(field, ascending))
    }
}

class OUnionSelect(
    override val className: String,
    val left: OClassSelect,
    val right: OClassSelect,
    override val order: OOrder? = null
) : OClassSelect {

    override val condition: OCondition? = null

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#unionall
    // intersect returns projection thus need to expand it into collection
    override fun sql() = "SELECT expand(unionall((${left.sql()}), (${right.sql()}))) ${order.orderByOrEmpty()}".trim()
    override fun params() = left.params() + right.params()

    override fun withOrder(field: String, ascending: Boolean): OClassSelect {
        return OUnionSelect(className, left, right, OOrderByField(field, ascending))
    }
}

fun OCondition?.whereOrEmpty() = this?.let { "WHERE ${it.sql()}" } ?: ""
fun OOrder?.orderByOrEmpty() = this?.let { "ORDER BY ${it.sql()}" } ?: ""