package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.Entity

interface OEntity : Entity {

    override fun getId(): OEntityId

    fun save(): OEntity
}
