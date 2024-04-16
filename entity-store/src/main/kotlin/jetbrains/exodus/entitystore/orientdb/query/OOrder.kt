package jetbrains.exodus.entitystore.orientdb.query

sealed interface OOrder : OSql {
    fun merge(order: OOrder): OOrder
}

data class OrderItem(val field: String, val ascending: Boolean = true)

class OOrderByFields(
    val items: List<OrderItem>
) : OOrder {

    constructor(field: String, ascending: Boolean = true) : this(listOf(OrderItem(field, ascending)))

    override fun sql() = items.map { (field, ascending) ->
        "$field ${if (ascending) "ASC" else "DESC"}"
    }.joinToString(", ")

    override fun merge(order: OOrder): OOrder {
        return when (order) {
            is OOrderByFields -> OOrderByFields(items + order.items)
        }
    }
}

fun OOrder?.merge(order: OOrder?): OOrder? {
    if (this == null) return order
    if (order == null) return this
    return this.merge(order)
}