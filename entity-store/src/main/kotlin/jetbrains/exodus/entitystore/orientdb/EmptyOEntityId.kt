package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
import jetbrains.exodus.entitystore.EntityId

internal class EmptyOEntityId : OEntityId {
    override fun getTypeId(): Int {
        throw UnsupportedOperationException()
    }

    override fun getLocalId(): Long {
        throw UnsupportedOperationException()
    }

    override fun asOId(): ORID {
        throw UnsupportedOperationException()
    }

    override fun compareTo(other: EntityId): Int {
        if (other is EmptyOEntityId) {
            return 0
        }

        return -1
    }

    override fun hashCode(): Int {
        return 81304394
    }

    override fun equals(other: Any?): Boolean {
        return other is EmptyOEntityId
    }

    override fun toString(): String {
        return "EmptyOId"
    }
}