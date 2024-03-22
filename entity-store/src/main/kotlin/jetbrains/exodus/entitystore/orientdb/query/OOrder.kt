package jetbrains.exodus.entitystore.orientdb.query

interface OOrder {

    fun sql(): String
}

class OOrderByField(
    val field: String,
    val ascending: Boolean = true
) : OOrder {

    override fun sql() = "$field ${if (ascending) "ASC" else "DESC"}"
}