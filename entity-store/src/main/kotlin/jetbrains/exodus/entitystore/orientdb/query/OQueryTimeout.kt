package jetbrains.exodus.entitystore.orientdb.query

class OQueryTimeout(
    val timeoutMillis: Long
) : OQuery {

    override fun sql(builder: StringBuilder) {
        builder.append(" TIMEOUT $timeoutMillis")
    }
}