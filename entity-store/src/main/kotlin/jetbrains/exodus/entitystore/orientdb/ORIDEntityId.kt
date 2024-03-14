package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.EntityId

class ORIDEntityId(private val id: ORID, private val schemaClass: OClass) : OEntityId {

    companion object {

        fun fromVertex(vertex: OVertex): ORIDEntityId {
            val schema = vertex.schemaClass
            require(schema != null) { "Vertex $vertex must have a schema class" }
            return ORIDEntityId(vertex.identity, schema)
        }
    }

    override fun asOId(): ORID {
        return id
    }

    override fun getTypeId(): Int {
        var id = 0
        for (c in schemaClass.name) {
            id = 31 * id + c.code
        }
        return id
    }

    fun getTypeName(): String {
        return schemaClass.name
    }

    override fun getLocalId(): Long {
        return id.clusterPosition
    }

    override fun compareTo(other: EntityId?): Int {
        if (other !is ORIDEntityId) {
            throw IllegalArgumentException("Cannot compare ORIDEntityId with ${other?.javaClass?.name}")
        }
        return id.compareTo(other.id)
    }

    override fun toString(): String {
        return "${schemaClass}:${id}"
    }
}