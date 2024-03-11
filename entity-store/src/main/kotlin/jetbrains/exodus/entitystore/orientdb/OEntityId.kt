package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.record.OIdentifiable
import jetbrains.exodus.entitystore.EntityId

interface OEntityId : EntityId {

    fun asOIdentifiable(): OIdentifiable
}