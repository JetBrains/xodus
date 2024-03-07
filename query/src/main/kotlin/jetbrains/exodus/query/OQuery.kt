package jetbrains.exodus.query

class OQuery(
    val select: OSelect,
    val condition: OCondition? = null
) {

    override fun toString(): String {
        return if (condition == null) {
            select.toString()
        } else {
            "$select WHERE $condition"
        }
    }
}

class OSelect(
    val from: String
) {
    override fun toString(): String {
        return "SELECT FROM $from"
    }
}


class OCondition(
    val field: String,
    val value: Any
) {
    override fun toString(): String {
        return "$field = ?$value"
    }
}