package jetbrains.exodus.entitystore.orientdb.query

interface OLimit: OSql

class OLimitValue(
    val value: Int
) : OLimit {

    override fun sql() = "$value"
}