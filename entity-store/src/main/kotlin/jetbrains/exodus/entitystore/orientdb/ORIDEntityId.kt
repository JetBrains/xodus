package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.EntityId

class ORIDEntityId(
    private val classId: Int,
    private val localEntityId: Long,
    private val oId: ORID,
    private val schemaClass: OClass
) : OEntityId {

    companion object {

        fun fromVertex(vertex: OVertex): ORIDEntityId {
            val oClass = vertex.schemaClass ?: throw IllegalStateException("schemaClass not found for $vertex")
            val classId = oClass.getCustom(OVertexEntity.CLASS_ID_CUSTOM_PROPERTY_NAME)?.toInt() ?: throw IllegalStateException("classId not found for ${oClass.name}")
            val localEntityId = vertex.getProperty<Long>(OVertexEntity.BACKWARD_COMPATIBLE_LOCAL_ENTITY_ID_PROPERTY_NAME) ?: throw IllegalStateException("localEntityId not found for the vertex")
            return ORIDEntityId(classId, localEntityId, vertex.identity, oClass)
        }
    }

    override fun asOId(): ORID {
        return oId
    }

    override fun getTypeId(): Int {
        return classId
    }

    fun getTypeName(): String {
        return schemaClass.name
    }

    override fun getLocalId(): Long {
        return localEntityId
    }

    override fun compareTo(other: EntityId?): Int {
        if (other !is ORIDEntityId) {
            throw IllegalArgumentException("Cannot compare ORIDEntityId with ${other?.javaClass?.name}")
        }
        return oId.compareTo(other.oId)
    }

    override fun toString(): String {
        return "${classId}-${localEntityId}"
    }

    override fun hashCode(): Int {
        return oId.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ORIDEntityId

        return oId == other.oId
    }
}
