package jetbrains.exodus.entitystore.orientdb.query

interface OSkip : OSql

class OSkipValue(
    val value: Int
) : OSkip {

    override fun sql() = "$value"
}