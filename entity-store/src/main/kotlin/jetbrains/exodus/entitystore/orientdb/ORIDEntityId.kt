package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.id.ORecordId
import jetbrains.exodus.entitystore.EntityId

class ORIDEntityId(private val id: ORID) : OEntityId {

    override fun getTypeId(): Int {
        return id.clusterId
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

    override fun asOIdentifiable(): OIdentifiable {
        return id
    }
}


// typeId -> clusterId, localId -> clusterPosition
// BUT, clusterId != typeId, typeId is more like a class
// More info: https://orientdb.com/docs/3.0.x/gettingstarted/Tutorial-Classes.html#working-with-classes
// ToDo: think it over
fun EntityId.toRecordId(): ORID = ORecordId(this.typeId, this.localId)