package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.record.OTrackedSet

class OComparableSet<E>(val source: MutableSet<E>) : MutableSet<E> by source, Comparable<MutableSet<E>> {

    val isDirty: Boolean
        get() {
            return if (source is OTrackedSet) {
                source.isTransactionModified
            } else {
                true
            }
        }

    override fun compareTo(other: MutableSet<E>): Int {
        return 0
    }
}
