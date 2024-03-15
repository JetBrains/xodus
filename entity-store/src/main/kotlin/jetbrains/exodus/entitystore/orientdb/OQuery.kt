package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
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
sealed interface OClassSelect : OQuery {
    val className: String
    val condition: OCondition?

    fun copyWithCondition(condition: OCondition?): OClassSelect
}

// Select extensions
private fun OClassSelect.ensureSelectAndSameDbClass(other: OQuery): OClassSelect {
    require(other is OClassSelect) { "Unsupported query type" }
    require(other.className == className) { "Cannot intersect different DB classes, expected: ${className}, but was ${other.className}" }
    return other
}

// Select implementations
class OSimpleSelect(
    override val className: String,
    override val condition: OCondition? = null
) : OClassSelect {

    override fun sql() = "SELECT from $className ${condition.whereOrEmpty()}"
    override fun params() = condition?.params() ?: emptyMap()

    override fun union(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameDbClass(other)
        val newCondition = condition.or(otherSelect.condition)
        // No need for checking other select as the current one is always the broadest
        return OSimpleSelect(className, newCondition)
    }

    override fun intersect(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameDbClass(other)
        val newCondition = condition.and(otherSelect.condition)
        return when (otherSelect) {
            is OLinkInSelect -> otherSelect.copyWithCondition(newCondition)
            is OSimpleSelect -> this.copyWithCondition(newCondition)
        }
    }

    override fun copyWithCondition(condition: OCondition?) = OSimpleSelect(className, condition)
}

class OLinkInSelect(
    override val className: String,
    val linkName: String,
    val targetId: ORID,
    override val condition: OCondition? = null
) : OClassSelect {

    override fun sql() = "SELECT expand(in('${linkName}')) from $targetId ${condition.whereOrEmpty()}"
    override fun params() = emptyMap<String, Any>()

    override fun copyWithCondition(condition: OCondition?) = OLinkInSelect(className, linkName, targetId, condition)

    override fun union(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameDbClass(other)
        val newCondition = condition.or(otherSelect.condition)
        return when (otherSelect) {
            is OSimpleSelect -> otherSelect.copyWithCondition(newCondition)
            is OLinkInSelect -> TODO("Union of two in-link selects is not supported yet")
        }
    }

    override fun intersect(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameDbClass(other)
        val newCondition = condition.and(otherSelect.condition)
        return when (otherSelect) {
            is OSimpleSelect -> this.copyWithCondition(newCondition)
            is OLinkInSelect -> TODO("Intersection of two in-link selects is not supported yet")
        }
    }
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