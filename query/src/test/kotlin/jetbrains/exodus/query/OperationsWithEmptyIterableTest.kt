package jetbrains.exodus.query

import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityIterableBase
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityOfTypeIterable
import jetbrains.exodus.entitystore.youtrackdb.testutil.*
import jetbrains.exodus.query.metadata.entity
import jetbrains.exodus.query.metadata.oModel
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals

class OperationsWithEmptyIterableTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB()

    override val youTrackDb = orientDbRule

    lateinit var testCase: OUsersWithInheritanceTestCase

    @Test
    fun operationsWithEmpty() {
        testCase = OUsersWithInheritanceTestCase(youTrackDb)

        val model = givenModel()

        val engine = QueryEngine(model, youTrackDb.store)
        engine.sortEngine = SortEngine()
        youTrackDb.withStoreTx { txn ->
            val users = YTDBEntityOfTypeIterable(txn, User.CLASS)
            assertEquals(YTDBEntityIterableBase.EMPTY, users.intersect(YTDBEntityIterableBase.EMPTY))
            assertEquals(YTDBEntityIterableBase.EMPTY, users.intersectSavingOrder(YTDBEntityIterableBase.EMPTY))
            assertEquals(users, users.concat(YTDBEntityIterableBase.EMPTY))
            assertEquals(users, users.union(YTDBEntityIterableBase.EMPTY))
            assertEquals(users, users.minus(YTDBEntityIterableBase.EMPTY))
        }
    }

    private fun givenModel() = oModel(youTrackDb.provider) {
        entity(BaseUser.CLASS)
        entity(User.CLASS, BaseUser.CLASS)
        entity(Admin.CLASS, BaseUser.CLASS)
        entity(Agent.CLASS, BaseUser.CLASS)
        entity(Guest.CLASS, BaseUser.CLASS)
    }.apply {
        prepare()
    }

}
