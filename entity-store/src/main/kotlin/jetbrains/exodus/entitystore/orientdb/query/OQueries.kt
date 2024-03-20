package jetbrains.exodus.entitystore.orientdb.query

object OQueries {

    fun intersect(left: OQuery, right: OQuery): OQuery {
        require(left is OClassSelect) { "Unsupported query type for $left" }
        require(right is OClassSelect) { "Unsupported query type for $right" }
        ensureSameClassName(left, right)

        return when {
            left is OAllSelect && right is OAllSelect -> {
                val newCondition = left.condition.and(right.condition)
                OAllSelect(left.className, newCondition)
            }

            else -> {
                OIntersectSelect(left.className, left, right)
            }
        }
    }

    fun union(left: OQuery, right: OQuery): OQuery {
        require(left is OClassSelect) { "Unsupported query type for $left" }
        require(right is OClassSelect) { "Unsupported query type for $right" }
        ensureSameClassName(left, right)

        return when {
            left is OAllSelect && right is OAllSelect -> {
                val newCondition = left.condition.or(right.condition)
                OAllSelect(left.className, newCondition)
            }

            else -> {
                OUnionSelect(left.className, left, right)
            }
        }
    }

    private fun ensureSameClassName(left: OClassSelect, right: OClassSelect) {
        require(left.className == right.className) { "Cannot intersect different DB classes: ${left.className} and ${right.className}" }
    }
}

