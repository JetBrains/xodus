package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.id.ORID
import jetbrains.exodus.entitystore.Entity

interface OEntity : Entity {

    fun getORID(): ORID
}