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
}

// Select extensions
private fun OClassSelect.ensureSelectAndSameClassName(other: OQuery): OClassSelect {
    require(other is OClassSelect) { "Unsupported query type" }
    require(other.className == className) { "Cannot intersect different DB classes, expected: ${className}, but was ${other.className}" }
    return other
}

// Select implementations
class OSimpleSelect(
    override val className: String,
    override val condition: OCondition? = null
) : OClassSelect {

    override fun sql() = "SELECT from $className ${condition.whereOrEmpty()}".trimEnd()
    override fun params() = condition?.params() ?: emptyMap()

    override fun union(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameClassName(other)
        val newCondition = condition.or(otherSelect.condition)
        // No need for checking other select as the current one is always the broadest
        return OSimpleSelect(className, newCondition)
    }

    override fun intersect(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameClassName(other)
        val newCondition = condition.and(otherSelect.condition)
        return when (otherSelect) {
            is OLinkInSelect -> otherSelect.copy(newCondition)
            is OSimpleSelect -> this.copy(newCondition)
            is OIntersectSelect -> { unsupported() }
            is OUnionSelect -> { unsupported() }
        }

    }

    fun copy(condition: OCondition?) = OSimpleSelect(className, condition)
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

    fun copy(condition: OCondition?) = OLinkInSelect(className, linkName, targetIds, condition)
    fun copy(targetIds: List<ORID>) = OLinkInSelect(className, linkName, targetIds, condition)

    override fun union(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameClassName(other)
        val orCondition = condition.or(otherSelect.condition)
        return when (otherSelect) {
            is OSimpleSelect -> otherSelect.copy(orCondition)
            is OLinkInSelect -> {
                if (otherSelect.linkName == linkName) {
                    this.copy(targetIds + otherSelect.targetIds).copy(orCondition)
                } else {
                    OUnionSelect(className, this, otherSelect)
                }
            }
            is OIntersectSelect -> { unsupported() }
            is OUnionSelect -> { unsupported() }
        }
    }

    override fun intersect(other: OQuery): OQuery {
        val otherSelect = ensureSelectAndSameClassName(other)
        return when (otherSelect) {
            is OSimpleSelect -> {
                val newCondition = condition.and(otherSelect.condition)
                this.copy(newCondition)
            }

            is OLinkInSelect -> {
                OIntersectSelect(className, this, otherSelect)
            }

            is OIntersectSelect -> { unsupported() }
            is OUnionSelect -> { unsupported() }
        }
    }
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