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
package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.TestUtil
import jetbrains.exodus.entitystore.*
import org.junit.Assert
import java.util.*

class OEntityTests : OEntityStoreTestBase() {


    fun testCreateSingleEntity() {
        createClasses(listOf("Issue"))

        val entity = entityStore.computeInTransaction { txn ->
            val entity = txn.newEntity("Issue")
            val all = txn.getAll("Issue")
            Assert.assertEquals(1, all.size())
            Assert.assertTrue(all.iterator().hasNext())
            Assert.assertNotNull(entity)
            entity
        }

        Assert.assertTrue(entity.id.typeId >= 0)
        Assert.assertTrue(entity.id.localId >= 0)
    }

    fun testCreateSingleEntity2() {
//        acquireSession().use {
//            it.createVertexClass("Issue")
//        }
//        transactional { txn ->
//            val entity = txn.newEntity("Issue")
//            txn.flush()
//            Assert.assertNotNull(entity)
//            Assert.assertTrue(entity.id.typeId >= 0)
//            Assert.assertTrue(entity.id.localId >= 0)
//            try {
//                txn.getEntity(
//                    PersistentEntityId(0, 1)
//                )
//                Assert.fail()
//            } catch (ignore: EntityRemovedInDatabaseException) {
//            }
//        }
    }


    fun testEntityIdToString() {
//        acquireSession().use {
//            it.createVertexClass("Issue")
//        }
//
//        transactional { txn ->
//            val entity = txn.newEntity("Issue")
//            txn.flush()
//            val representation = entity.id.toString()
//            Assert.assertEquals(entity, txn.getEntity(txn.toEntityId(representation)))
//        }
    }

    fun testCreateTwoEntitiesInTransaction() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity1 = txn.newEntity("Issue")
            val entity2 = txn.newEntity("Issue")
            txn.flush()
            Assert.assertNotNull(entity1)
            Assert.assertTrue(entity1.id.typeId >= 0)
            Assert.assertTrue(entity1.id.localId >= 0)
            Assert.assertNotNull(entity2)
            Assert.assertTrue(entity2.id.localId > 0)
            Assert.assertTrue(entity2.id.localId > entity1.id.localId)
        }
    }

    fun testCreateTwoEntitiesInTwoTransactions() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity1 = txn.newEntity("Issue")
            txn.flush()
            val entity2 = txn.newEntity("Issue")
            txn.flush()
            Assert.assertNotNull(entity1)
            Assert.assertTrue(entity1.id.typeId >= 0)
            Assert.assertTrue(entity1.id.localId >= 0)
            Assert.assertNotNull(entity2)
            Assert.assertTrue(entity2.id.localId > 0)
            Assert.assertTrue(entity2.id.localId > entity1.id.localId)
        }
    }

    fun testCreateAndGetSingleEntity() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity = txn.newEntity("Issue")
            txn.flush()

            Assert.assertEquals("Issue", entity.type)
            val sameEntity = txn.getEntity(entity.id)

            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
        }
    }

    fun testIntProperty() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity = txn.newEntity("Issue")
            entity.setProperty("size", 100)
            entity.setProperty("minus_size", -100)
            txn.flush()
            Assert.assertEquals("Issue", entity.type)
            val sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            Assert.assertEquals(100, entity.getProperty("size"))
            Assert.assertEquals(-100, entity.getProperty("minus_size"))
        }
    }

    fun testLongProperty() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity = txn.newEntity("Issue")
            entity.setProperty("length", 0x10000ffffL)
            txn.flush()
            Assert.assertEquals("Issue", entity.type)
            val sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            Assert.assertEquals(0x10000ffffL, entity.getProperty("length"))
        }
    }

    fun testStringProperty() {
        createClasses(listOf("Issue"))
        transactional { txn ->
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "This is a test issue")
            txn.flush()
            Assert.assertEquals("Issue", entity.type)
            val sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            Assert.assertEquals("This is a test issue", entity.getProperty("description"))
        }
    }

    fun testDoubleAndFloatProperties() {
        createClasses(listOf("Issue"))
        transactional { txn ->
            val entity = txn.newEntity("Issue")
            entity.setProperty("hitRate", 0.123456789)
            entity.setProperty("hitRate (float)", 0.123456789f)
            entity.setProperty("crude oil (WTI) price", -40.32)
            entity.setProperty("crude oil (WTI) price (float)", -40.32f)
            txn.flush()
            Assert.assertEquals("Issue", entity.type)
            val sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            Assert.assertEquals(0.123456789, entity.getProperty("hitRate"))
            Assert.assertEquals(0.123456789f, entity.getProperty("hitRate (float)"))
            Assert.assertEquals(-40.32, entity.getProperty("crude oil (WTI) price"))
            Assert.assertEquals(-40.32f, entity.getProperty("crude oil (WTI) price (float)"))
        }
    }

    fun testDateProperty() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity = txn.newEntity("Issue")
            val date = Date()
            entity.setProperty("date", date.time)
            txn.flush()
            Assert.assertEquals("Issue", entity.type)
            val sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            val dateProp = entity.getProperty("date")
            Assert.assertNotNull(dateProp)
            Assert.assertEquals(date.time, dateProp)
            Assert.assertTrue(Date().time >= dateProp as Long)
        }
    }

    fun testBooleanProperty() {
        createClasses(listOf("Issue"))
        transactional { txn ->
            val entity = txn.newEntity("Issue")
            entity.setProperty("ready", true)
            txn.flush()
            Assert.assertEquals("Issue", entity.type)
            var sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            Assert.assertTrue(entity.getProperty("ready") as Boolean)
            entity.setProperty("ready", false)
            txn.flush()
            sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            Assert.assertFalse(entity.getProperty("ready") as Boolean)
        }
    }

    fun testHeterogeneousProperties() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "This is a test issue")
            entity.setProperty("size", 100)
            entity.setProperty("rank", 0.5)
            txn.flush()
            Assert.assertEquals("Issue", entity.type)
            val sameEntity = txn.getEntity(entity.id)
            Assert.assertNotNull(sameEntity)
            Assert.assertEquals(entity.type, sameEntity.type)
            Assert.assertEquals(entity.id, sameEntity.id)
            Assert.assertEquals("This is a test issue", entity.getProperty("description"))
            Assert.assertEquals(100, entity.getProperty("size"))
            Assert.assertEquals(0.5, entity.getProperty("rank"))
        }
    }

    fun testOverwriteProperty() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "This is a test issue")
            txn.flush()
            Assert.assertEquals("This is a test issue", entity.getProperty("description"))
            entity.setProperty("description", "This is overriden test issue")
            txn.flush()
            Assert.assertEquals("This is overriden test issue", entity.getProperty("description"))
            entity.deleteProperty("description") // for XD-262 I optimized this to prohibit such stuff
            entity.setProperty("description", 100)
            txn.flush()
            Assert.assertEquals(100, entity.getProperty("description"))
        }
    }

    fun testDeleteProperty() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            val issue = txn.newEntity("Issue")
            issue.setProperty("description", "This is a test issue")
            txn.flush()
            Assert.assertEquals("This is a test issue", issue.getProperty("description"))
            issue.deleteProperty("description")
            txn.flush()
            Assert.assertNull(issue.getProperty("description"))
            val issues = txn.find("Issue", "description", "This is a test issue")
            Assert.assertFalse(issues.iterator().hasNext())
        }
    }

    fun testReadingWithoutTransaction() {
        acquireSession().use {
            it.createVertexClassWithClassId("Issue")
            it.createVertexClassWithClassId("User")
            it.createLightweightEdgeClass("creator".asEdgeClass)
        }

        transactional { txn ->
            txn.getAll("Issue")
            try {
                val issue = txn.newEntity("Issue")
                issue.setProperty("name", "my name")
                val user = txn.newEntity("User")
                user.setProperty("name", "charisma user")
                issue.addLink("creator", user)
            } finally {
                txn.flush()
            }
        }

        transactional { txn ->
            for (issue in txn.getAll("Issue")) {
                Assert.assertEquals("my name", issue.getProperty("name"))
                val users: Iterable<Entity> = issue.getLinks("creator")
                for (user in users) {
                    Assert.assertEquals("charisma user", user.getProperty("name"))
                }
            }
        }
    }

    fun testDeleteEntities() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            txn.newEntity("Issue")
            txn.newEntity("Issue")
            txn.newEntity("Issue")
            txn.newEntity("Issue")
            txn.flush()

            for ((i, issue) in txn.getAll("Issue").withIndex()) {
                if (i and 1 == 0) {
                    issue.delete()
                }
            }

            txn.flush()
            Assert.assertEquals(2L, txn.getAll("Issue").size())
        }
    }

    fun testRenameEntityType() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            for (i in 0..9) {
                txn.newEntity("Issue")
            }
            txn.flush()
            Assert.assertEquals(10, txn.getAll("Issue").size())
        }
        entityStore.renameEntityType("Issue", "Comment")
        transactional { txn ->
            Assert.assertEquals(10, txn.getAll("Comment").size())
        }
    }

    fun testRenameNonExistingEntityType() {
        createClasses(listOf("Issue"))

        transactional { txn ->
            for (i in 0..9) {
                txn.newEntity("Issue")
            }
            txn.flush()
            Assert.assertEquals(10, txn.getAll("Issue").size())
        }

        TestUtil.runWithExpectedException(
            { entityStore.renameEntityType("Comment", "Issue") },
            IllegalArgumentException::class.java
        )
    }

    @Throws(InterruptedException::class)
    fun testConcurrentCreationTypeIdsAreOk() {
        val count = 100
        val itsOk = booleanArrayOf(true)
        val target = Runnable {
            val i = intArrayOf(0)
            while (i[0] <= count) {
                if (!entityStore.computeInTransaction { txn ->
                        val e = txn.newEntity("Entity" + i[0])
                        if (e.id.typeId != i[0]) {
                            itsOk[0] = false
                            return@computeInTransaction false
                        }
                        true
                    }) {
                    break
                }
                ++i[0]
            }
        }
        val t1 = Thread(target)
        val t2 = Thread(target)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        Assert.assertTrue(itsOk[0])
    }

    fun testTxnCachesIsolation() {
        createClasses(listOf("Issue"))

        val issue = entityStore.computeInTransaction { txn ->
            txn.newEntity("Issue").apply { setProperty("description", "1") }
        }



        transactional {
            Assert.assertEquals("1", issue.getProperty("description"))
        }

        entityStore.executeInTransaction {
            issue.setProperty("description", "2")
        }

        transactional {
            Assert.assertEquals("2", issue.getProperty("description"))
        }
    }

    fun testTxnCachesIsolation2() {
        createClasses(listOf("Issue"))

        val issue = entityStore.computeInTransaction { txn ->
            txn.newEntity("Issue").apply { setProperty("description", "1") }
        }

        transactional { txn ->
            Assert.assertEquals("1", issue.getProperty("description"))
            issue.setProperty("description", "2")

            entityStore.executeInTransaction {
                issue.setProperty("description", "3")
            }

            Assert.assertEquals("3", issue.getProperty("description"))
        }
    }

    private fun createClasses( vectorClasses: Collection<String>, edgeClasses: Collection<String> = listOf()) {
        acquireSession().use {
            vectorClasses.forEach { name -> it.createVertexClassWithClassId(name) }
            edgeClasses.forEach { name -> it.createLightweightEdgeClass(name) }
        }
        schemaBuddy.initialize()
    }
}
