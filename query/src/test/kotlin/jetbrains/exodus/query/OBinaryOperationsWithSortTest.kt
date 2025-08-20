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

import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterableImpl
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityOfTypeIterable
import jetbrains.exodus.entitystore.youtrackdb.testutil.*
import jetbrains.exodus.query.metadata.entity
import jetbrains.exodus.query.metadata.oModel
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertContentEquals

class OBinaryOperationsWithSortTest : OTestMixin {
    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB()

    override val youTrackDb = orientDbRule

    lateinit var testCase: OUsersWithInheritanceTestCase

    @Test
    fun union() {
        testCase = OUsersWithInheritanceTestCase(youTrackDb)

        val model = givenModel()

        val engine = QueryEngine(model, youTrackDb.store)
        engine.sortEngine = SortEngine()
        youTrackDb.withStoreTx { txn ->
            val users = GremlinEntityIterableImpl(txn, GremlinQuery.all.then(GremlinBlock.HasLabel(User.CLASS)))
            val agents = GremlinEntityIterableImpl(txn, GremlinQuery.all.then(GremlinBlock.HasLabel(Agent.CLASS)))
            val union = engine.union(users, agents)
            val sorted = engine.query(union, BaseUser.CLASS, NodeFactory.sortBy("name", GremlinBlock.SortDirection.ASC))
            assertContentEquals(
                listOf("u1", "u2"),
                sorted.skip(2).take(2).toList().map { it.getProperty("name") })
        }
    }


    @Test
    fun intersect() {
        testCase = OUsersWithInheritanceTestCase(youTrackDb)

        val model = givenModel()

        val engine = QueryEngine(model, youTrackDb.store)
        engine.sortEngine = SortEngine()
        youTrackDb.withStoreTx { txn ->
            txn.createUser(Agent.CLASS, "u1")
        }
        youTrackDb.withStoreTx { txn ->
            val users = engine.query(User.CLASS, NodeFactory.propEqual("name", "u1"))
            val all = engine.query(BaseUser.CLASS, NodeFactory.propEqual("name", "u1"))
            val intersect = engine.intersect(users, all)
            val sorted =
                engine.query(intersect, BaseUser.CLASS, NodeFactory.sortBy("name", GremlinBlock.SortDirection.ASC))
            Assert.assertEquals(2, all.size())
            assertContentEquals(
                listOf("u1"),
                sorted.toList().map { it.getProperty("name") })
        }
    }

    @Test
    fun minus() {
        testCase = OUsersWithInheritanceTestCase(youTrackDb)

        val model = givenModel()

        val engine = QueryEngine(model, youTrackDb.store)
        engine.sortEngine = SortEngine()
        youTrackDb.withStoreTx { txn ->
            val users = engine.query(User.CLASS, NodeFactory.all())
            val u1 = engine.query(BaseUser.CLASS, NodeFactory.propEqual("name", "u1"))
            val minus = engine.exclude(users, u1)
            val sorted = engine.query(minus, BaseUser.CLASS, NodeFactory.sortBy("name", GremlinBlock.SortDirection.ASC))
            Assert.assertEquals(1, minus.count())
            assertContentEquals(
                listOf("u2"),
                sorted.toList().map { it.getProperty("name") })
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
