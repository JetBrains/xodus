package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.orient.core.id.ORID

sealed interface OClassSelect : OQuery {
    val className: String
    val condition: OCondition?
}

class OAllSelect(
    override val className: String,
    override val condition: OCondition? = null
) : OClassSelect {

    override fun sql() = "SELECT from $className ${condition.whereOrEmpty()}".trimEnd()
    override fun params() = condition?.params() ?: emptyList()
}

class OLinkInSelect(
    override val className: String,
    val linkName: String,
    val targetIds: List<ORID>,
    override val condition: OCondition? = null
) : OClassSelect {

    override fun sql() = "SELECT expand(in('$linkName')) from $targetIdsSql ${condition.whereOrEmpty()}".trimEnd()

    private val targetIdsSql get() = "[${targetIds.map(ORID::toString).joinToString(", ")}]"
}

class OIntersectSelect(
    override val className: String,
    val left: OClassSelect,
    val right: OClassSelect,
    override val condition: OCondition? = null
) : OClassSelect {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#intersect
    // intersect returns projection thus need to expand it into collection
    override fun sql() = "SELECT expand(intersect((${left.sql()}), (${right.sql()})))"
    override fun params() = left.params() + right.params()
}

class OUnionSelect(
    override val className: String,
    val left: OClassSelect,
    val right: OClassSelect,
    override val condition: OCondition? = null
) : OClassSelect {

    // https://orientdb.com/docs/3.2.x/sql/SQL-Functions.html#unionall
    // intersect returns projection thus need to expand it into collection
    override fun sql() = "SELECT expand(unionall((${left.sql()}), (${right.sql()})))"
    override fun params() = left.params() + right.params()
}

fun OCondition?.whereOrEmpty() = this?.let { "WHERE ${it.sql()}" } ?: ""