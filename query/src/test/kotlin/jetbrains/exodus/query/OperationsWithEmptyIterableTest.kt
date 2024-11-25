/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityIterableBase
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityOfTypeIterable
import jetbrains.exodus.entitystore.youtrackdb.testutil.*
import jetbrains.exodus.query.metadata.entity
import jetbrains.exodus.query.metadata.oModel
import org.junit.Assert
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

    @Test
    fun `query for non existent class should return empty YTDBEmpty`(){
        val model = givenModel()
        val engine = QueryEngine(model, youTrackDb.store)
        withStoreTx {
            val iterable = engine.queryGetAll("HAHAHA_NOSUCH_CLASS")
            Assert.assertEquals(iterable.size(), 0)
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
