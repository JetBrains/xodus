package jetbrains.exodus.entitystore.orientdb.query

object OQueryFunctions {

    fun intersect(left: OSelect, right: OSelect): OSelect {
        return when {
            left is OClassSelect && right is OClassSelect -> {
                ensureSameClassName(left, right)
                val newCondition = left.condition.and(right.condition)
                OClassSelect(left.className, newCondition)
            }

            else -> {
                OIntersectSelect(left, right)
            }
        }
    }

    fun union(left: OSelect, right: OSelect): OSelect {
        return when {
            left is OClassSelect && right is OClassSelect -> {
                ensureSameClassName(left, right)
                val newCondition = left.condition.or(right.condition)
                OClassSelect(left.className, newCondition)
            }

            else -> {
                OUnionSelect(left, right)
            }
        }
    }

    fun difference(left: OSelect, right: OSelect): OSelect {
        return ODifferenceSelect(left, right)
    }

    fun distinct(source: OSelect): OSelect {
        return ODistinctSelect(source)
    }

    private fun ensureSameClassName(left: OClassSelect, right: OClassSelect) {
        require(left.className == right.className) { "Cannot intersect different DB classes: ${left.className} and ${right.className}" }
    }
}