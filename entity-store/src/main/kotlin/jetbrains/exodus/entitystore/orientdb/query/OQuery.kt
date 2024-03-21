package jetbrains.exodus.entitystore.orientdb.query

interface OQuery {
    fun sql(): String
    fun params(): List<Any> = emptyList<Any>()
}
