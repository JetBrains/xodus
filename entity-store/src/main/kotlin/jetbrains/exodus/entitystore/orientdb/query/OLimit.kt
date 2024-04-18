package jetbrains.exodus.entitystore.orientdb.query

sealed interface OLimit : OSql {
    fun min(other: OLimit): OLimit
    fun max(other: OLimit): OLimit
}

class OLimitValue(
    val value: Int
) : OLimit {

    override fun sql() = "$value"

    override fun min(other: OLimit): OLimit {
        return when (other) {
            is OLimitValue -> if (this.value < other.value) this else other
        }
    }

    override fun max(other: OLimit): OLimit {
        return when (other) {
            is OLimitValue -> if (this.value > other.value) this else other
        }
    }
}

fun OLimit?.min(other: OLimit?): OLimit? {
    if (this == null) return other
    if (other == null) return this
    return this.min(other)
}

fun OLimit?.max(other: OLimit?): OLimit? {
    if (this == null) return other
    if (other == null) return this
    return this.max(other)
}