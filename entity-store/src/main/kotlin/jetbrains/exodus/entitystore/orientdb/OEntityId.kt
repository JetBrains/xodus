package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.id.ORecordId
import jetbrains.exodus.entitystore.EntityId

interface OEntityId : EntityId {

    fun asOIdentifiable(): OIdentifiable
}


fun EntityId.toRecordId() = ORecordId(this.typeId, this.localId)