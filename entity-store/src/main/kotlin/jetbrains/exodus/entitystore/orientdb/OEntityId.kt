package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
import jetbrains.exodus.entitystore.EntityId

interface OEntityId : EntityId {

    fun asOId(): ORID
}