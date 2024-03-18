package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
import java.util.UUID

// Basic
sealed interface OQuery {
    fun sql(): String
    fun params(): Map<String, Any>
}

object OQueries {

    fun intersect(left: OQuery, right: OQuery): OQuery {
        require(left is OClassSelect) { "Unsupported query type for $left" }
        require(right is OClassSelect) { "Unsupported query type for $right" }
        ensureSameClassName(left, right)

        return when {
            left is OAllSelect && right is OAllSelect -> {
                val newCondition = left.condition.and(right.condition)
                OAllSelect(left.className, newCondition)
            }

            else -> {
                OIntersectSelect(left.className, left, right)
            }
        }
    }

    fun union(left: OQuery, right: OQuery): OQuery {
        require(left is OClassSelect) { "Unsupported query type for $left" }
        require(right is OClassSelect) { "Unsupported query type for $right" }
        ensureSameClassName(left, right)

        return when {
            left is OAllSelect && right is OAllSelect -> {
                val newCondition = left.condition.or(right.condition)
                OAllSelect(left.className, newCondition)
            }

            else -> {
                OUnionSelect(left.className, left, right)
            }
        }
    }

    private fun ensureSameClassName(left: OClassSelect, right: OClassSelect) {
        require(left.className == right.className) { "Cannot intersect different DB classes: ${left.className} and ${right.className}" }
    }
}

// Select
sealed interface OClassSelect : OQuery {
    val className: String
    val condition: OCondition?
}

// Select implementations
class OAllSelect(
    override val className: String,
    override val condition: OCondition? = null
) : OClassSelect {

    override fun sql() = "SELECT from $className ${condition.whereOrEmpty()}".trimEnd()
    override fun params() = condition?.params() ?: emptyMap()
}

class OLinkInSelect(
    override val className: String,
    val linkName: String,
    val targetIds: List<ORID>,
    override val condition: OCondition? = null
) : OClassSelect {

    override fun sql() = "SELECT expand(in('$linkName')) from $targetIdsSql ${condition.whereOrEmpty()}".trimEnd()
    override fun params() = emptyMap<String, Any>()

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

// Where
sealed interface OCondition : OQuery

// Property
class OEqualCondition(
    val field: String,
    val value: Any,
) : OCondition {

    // ToDo: make paramId deterministic to leverage query parsing cache
    val paramId = "${field}_${UUID.randomUUID().toString().take(4).replace("-", "")}"

    override fun sql() = "$field = :$paramId"
    override fun params() = mapOf(paramId to value)
}

// Binary
sealed class OBiCondition(val operation: String, val left: OCondition, val right: OCondition) : OCondition {

    override fun sql() = "(${left.sql()} $operation ${right.sql()})"
    override fun params() = left.params() + right.params()
}

class OAndCondition(left: OCondition, right: OCondition) : OBiCondition("AND", left, right)
class OOrCondition(left: OCondition, right: OCondition) : OBiCondition("OR", left, right)

// Condition extensions
fun OCondition?.whereOrEmpty() = this?.let { "WHERE ${it.sql()}" } ?: ""

fun OCondition?.and(other: OCondition?): OCondition? {
    if (this == null) return other
    if (other == null) return this
    return OAndCondition(this, other)
}

fun OCondition?.or(other: OCondition?): OCondition? {
    if (this == null) return other
    if (other == null) return this
    return OOrCondition(this, other)
}

fun equal(field: String, value: Any) = OEqualCondition(field, value)
fun or(left: OCondition, right: OCondition) = OOrCondition(left, right)
fun and(left: OCondition, right: OCondition) = OAndCondition(left, right)