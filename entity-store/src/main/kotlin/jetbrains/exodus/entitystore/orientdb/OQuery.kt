package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.util.unsupported
import java.util.UUID

// Basic
interface OQuery {
    fun sql(): String
    fun params(): Map<String, Any>

    fun union(other: OQuery): OQuery = unsupported("Union is not supported by default")
    fun intersect(other: OQuery): OQuery = unsupported("Intersection is not supported by default")
}

// Select
class OClassSelect(
    val className: String,
    val condition: OCondition? = null
) : OQuery {

    override fun sql() = "SELECT from $className ${condition?.let { "WHERE ${it.sql()}" }}"
    override fun params() = condition?.params() ?: emptyMap()

    override fun union(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameDbClass(other)
        val newCondition = condition.or(otherSelect.condition)
        return OClassSelect(className, newCondition)
    }

    override fun intersect(other: OQuery): OClassSelect {
        val otherSelect = ensureSelectAndSameDbClass(other)
        val newCondition = condition.and(otherSelect.condition)
        return OClassSelect(className, newCondition)
    }

    private fun ensureSelectAndSameDbClass(other: OQuery): OClassSelect {
        if (other !is OClassSelect) throw IllegalArgumentException("Unsupported query type")
        if (other.className != className) throw IllegalArgumentException("Cannot intersect different DB classes")
        return other
    }
}


// Where
interface OCondition : OQuery

class OEqualCondition(
    val field: String,
    val value: Any,
) : OCondition {

    val paramId = "${field}_${UUID.randomUUID().toString().take(4).replace("-", "")}"

    override fun sql() = "$field = :$paramId"
    override fun params() = mapOf(paramId to value)
}

abstract class OBiCondition(val operation: String, val left: OCondition, val right: OCondition) : OCondition {

    override fun sql() = "(${left.sql()} $operation ${right.sql()})"
    override fun params() = left.params() + right.params()
}

class OAndCondition(left: OCondition, right: OCondition) : OBiCondition("AND", left, right)
class OOrCondition(left: OCondition, right: OCondition) : OBiCondition("OR", left, right)

// Extensions
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