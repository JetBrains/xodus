package jetbrains.exodus.entitystore.orientdb.testutil

import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OVertexEntity

object BaseUser {
    const val CLASS = "BaseUser"

    object Props {
        const val NAME = "login"
    }
}

object Guest {
    const val CLASS = "Guest"
}

object User {
    const val CLASS = "User"
}

object Agent {
    const val CLASS = "Agent"
}

object Admin {
    const val CLASS = "Admin"
}


fun OStoreTransaction.createUser(userClass: String, name: String): OVertexEntity {
    return newEntity(userClass).apply {
        setProperty("name", name)
    } as OVertexEntity
}
