package jetbrains.exodus.entitystore.orientdb.query

sealed interface OSkip : OSql {
    fun min(other: OSkip): OSkip
    fun max(other: OSkip): OSkip
}


class OSkipValue(
    val value: Int
) : OSkip {

    override fun sql() = "$value"

    override fun min(other: OSkip): OSkip {
        return when (other) {
            is OSkipValue -> if (this.value < other.value) this else other
        }
    }

    override fun max(other: OSkip): OSkip {
        return when (other) {
            is OSkipValue -> if (this.value > other.value) this else other
        }
    }
}


fun OSkip?.min(other: OSkip?): OSkip? {
    if (this == null) return other
    if (other == null) return this
    return this.min(other)
}

fun OSkip?.max(other: OSkip?): OSkip? {
    if (this == null) return other
    if (other == null) return this
    return this.max(other)
}