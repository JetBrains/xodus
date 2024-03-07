package jetbrains.exodus.entitystore.orientdb

interface OQuery {
    fun sql(): String
    fun params(): Map<String, Any>
}

class OClassSelect(
    val className: String,
    val condition: OQuery? = null
) : OQuery {

    override fun sql(): String {
        return if (condition == null) {
            "SELECT from $className"
        } else {
            "SELECT from $className WHERE ${condition.sql()}"
        }
    }

    override fun params(): Map<String, Any> {
        return condition?.params() ?: emptyMap()
    }


}

class OEqualCondition(
    val field: String,
    val value: Any
) : OQuery {

    override fun sql() = "$field = :$field"
    override fun params() = mapOf(field to value)
}

class OAndCondition(
    val conditions: List<OQuery>
) : OQuery {

    override fun sql() = conditions.joinToString(" AND ") { it.sql() }
    // ToDo
    override fun params() = conditions.first().params()
}

class OOrCondition(
    val conditions: List<OQuery>
) : OQuery {

    override fun sql() = conditions.joinToString(" OR ") { it.sql() }
    override fun params() = conditions.first().params()
}