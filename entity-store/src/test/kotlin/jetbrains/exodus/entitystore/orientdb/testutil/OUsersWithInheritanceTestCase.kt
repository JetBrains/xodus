package jetbrains.exodus.entitystore.orientdb.testutil

import jetbrains.exodus.entitystore.orientdb.OStoreTransactionImpl
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.getOrCreateVertexClass

class OUsersWithInheritanceTestCase(val orientDB: InMemoryOrientDB) {

    val user1: OVertexEntity
    val user2: OVertexEntity


    val agent1: OVertexEntity
    val agent2: OVertexEntity

    val guest: OVertexEntity

    val admin1: OVertexEntity
    val admin2: OVertexEntity


    init {

        orientDB.withSession { session ->
            val baseClass = session.getOrCreateVertexClass(BaseUser.CLASS)
            val subclasses = listOf(
                session.getOrCreateVertexClass(Guest.CLASS),
                session.getOrCreateVertexClass(User.CLASS),
                session.getOrCreateVertexClass(Admin.CLASS),
                session.getOrCreateVertexClass(Agent.CLASS),
            )
            subclasses.forEach {
                it.addSuperClass(baseClass)
            }
        }

        val tx = orientDB.store.beginTransaction() as OStoreTransactionImpl
        user1 = tx.createUser(User.CLASS, "u1")
        user2 = tx.createUser(User.CLASS, "u2")

        agent1 = tx.createUser(Agent.CLASS, "ag1")
        agent2 = tx.createUser(Agent.CLASS, "ag2")

        admin1 = tx.createUser(Admin.CLASS, "ad1")
        admin2 = tx.createUser(Admin.CLASS, "ad2")

        guest = tx.createUser(Guest.CLASS, "g1")
        tx.commit()
    }
}
