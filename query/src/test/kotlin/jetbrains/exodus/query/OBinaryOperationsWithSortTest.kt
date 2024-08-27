package jetbrains.exodus.query

import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityOfTypeIterable
import jetbrains.exodus.entitystore.orientdb.testutil.*
import jetbrains.exodus.query.metadata.entity
import jetbrains.exodus.query.metadata.oModel
import org.junit.Assert
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertContentEquals

class OBinaryOperationsWithSortTest : OTestMixin {
    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    lateinit var testCase: OUsersWithInheritanceTestCase

    @Test
    fun union() {
        testCase = OUsersWithInheritanceTestCase(orientDb)

        val model = givenModel()

        val engine = QueryEngine(model, orientDb.store)
        engine.sortEngine = SortEngine()
        orientDb.withStoreTx { txn ->
            val users = OEntityOfTypeIterable(txn, User.CLASS)
            val agents = OEntityOfTypeIterable(txn, Agent.CLASS)
            val union = engine.union(users, agents)
            val sorted = engine.query(union, BaseUser.CLASS, SortByProperty(null, "name", true))
            assertContentEquals(
                listOf("u1", "u2"),
                (sorted as EntityIterable).skip(2).take(2).toList().map { it.getProperty("name") })
        }
    }


    @Test
    @Ignore
    fun intersect() {
        testCase = OUsersWithInheritanceTestCase(orientDb)

        val model = givenModel()

        val engine = QueryEngine(model, orientDb.store)
        engine.sortEngine = SortEngine()
        orientDb.withStoreTx { txn ->
            txn.createUser(Agent.CLASS, "u1")
        }
        orientDb.withStoreTx { txn ->
            val users = engine.query(User.CLASS, PropertyEqual("name", "u1"))
            val all = engine.query(BaseUser.CLASS, PropertyEqual("name", "u1"))
            val intersect = engine.intersect(users, all)
            val sorted = engine.query(intersect, BaseUser.CLASS, SortByProperty(null, "name", true))
            Assert.assertEquals(2, all.size())
            assertContentEquals(
                listOf("u1"),
                (sorted as EntityIterable).toList().map { it.getProperty("name") })
        }
    }

    @Test
    @Ignore
    fun minus() {
        testCase = OUsersWithInheritanceTestCase(orientDb)

        val model = givenModel()

        val engine = QueryEngine(model, orientDb.store)
        engine.sortEngine = SortEngine()
        orientDb.withStoreTx { txn ->
            val users = OEntityOfTypeIterable(txn, User.CLASS)
            val u1 = engine.query(BaseUser.CLASS, PropertyEqual("name", "u1"))
            val minus = engine.exclude(users, u1)
            val sorted = engine.query(minus, BaseUser.CLASS, SortByProperty(null, "name", true))
            Assert.assertEquals(1, minus.count())
            assertContentEquals(
                listOf("u2"),
                (sorted as EntityIterable).toList().map { it.getProperty("name") })
        }
    }


    private fun givenModel() = oModel(orientDb.provider) {
        entity(BaseUser.CLASS)
        entity(User.CLASS, BaseUser.CLASS)
        entity(Admin.CLASS, BaseUser.CLASS)
        entity(Agent.CLASS, BaseUser.CLASS)
        entity(Guest.CLASS, BaseUser.CLASS)
    }.apply {
        prepare()
    }


}
