package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
import jetbrains.exodus.entitystore.Entity

interface OEntity : Entity {

    override fun getId(): OEntityId

    fun getOId(): ORID

    fun save()
}
