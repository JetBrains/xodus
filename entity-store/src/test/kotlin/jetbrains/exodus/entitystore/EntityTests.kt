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
package jetbrains.exodus.entitystore

import jetbrains.exodus.ExodusException
import jetbrains.exodus.TestFor
import jetbrains.exodus.TestUtil
import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.newEnvironmentConfig
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.log.TooBigLoggableException
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.DeferredIO
import jetbrains.exodus.util.LightByteArrayOutputStream
import jetbrains.exodus.util.UTFUtil
import junit.framework.TestCase
import org.junit.Assert
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore

@Suppress("UNCHECKED_CAST")
class EntityTests : EntityStoreTestBase() {

    override fun casesThatDontNeedExplicitTxn(): Array<String> {
        return arrayOf("testConcurrentCreationTypeIdsAreOk",
                "testConcurrentSerializableChanges",
                "testEntityStoreClear",
                "testSetPhantomLink",
                "testAddPhantomLink",
                "testTooBigProperty",
                "testTransactionAt",
                "testTransactionAtIsIdempotent"
        )
    }

    fun testCreateSingleEntity() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        val all = txn.getAll("Issue")
        Assert.assertEquals(1, all.size())
        Assert.assertTrue(all.iterator().hasNext())
        Assert.assertNotNull(entity)
        Assert.assertTrue(entity.id.typeId >= 0)
        Assert.assertTrue(entity.id.localId >= 0)
    }

    fun testCreateSingleEntity2() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        txn.flush()
        Assert.assertNotNull(entity)
        Assert.assertTrue(entity.id.typeId >= 0)
        Assert.assertTrue(entity.id.localId >= 0)
        Assert.assertEquals(entity.id, PersistentEntityId(0, 0))
        try {
            txn.getEntity(PersistentEntityId(0, 1))
            Assert.fail()
        } catch (ignore: EntityRemovedInDatabaseException) {
        }
    }

    fun testEntityIdToString() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        txn.flush()
        val representation = entity.id.toString()
        Assert.assertEquals(entity, txn.getEntity(txn.toEntityId(representation)))
    }

    fun testCreateTwoEntitiesInTransaction() {
        val txn = storeTransaction
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

    fun testCreateTwoEntitiesInTwoTransactions() {
        val txn = storeTransaction
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

    fun testCreateAndGetSingleEntity() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        txn.flush()
        Assert.assertEquals("Issue", entity.type)
        val sameEntity = txn.getEntity(entity.id)
        Assert.assertNotNull(sameEntity)
        Assert.assertEquals(entity.type, sameEntity.type)
        Assert.assertEquals(entity.id, sameEntity.id)
    }

    fun testRawProperty() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        entity.setProperty("description", "it doesn't work")
        txn.flush()
        Assert.assertEquals("Issue", entity.type)
        var sameEntity = txn.getEntity(entity.id)
        Assert.assertNotNull(sameEntity)
        Assert.assertEquals(entity.type, sameEntity.type)
        Assert.assertEquals(entity.id, sameEntity.id)
        var rawValue = entity.getRawProperty("description")
        Assert.assertNotNull(rawValue)
        Assert.assertEquals("it doesn't work", entityStore.propertyTypes.entryToPropertyValue(rawValue.notNull).data)
        entity.setProperty("description", "it works")
        txn.flush()
        sameEntity = txn.getEntity(entity.id)
        Assert.assertNotNull(sameEntity)
        Assert.assertEquals(entity.type, sameEntity.type)
        Assert.assertEquals(entity.id, sameEntity.id)
        rawValue = entity.getRawProperty("description")
        Assert.assertNotNull(rawValue)
        Assert.assertEquals("it works", entityStore.propertyTypes.entryToPropertyValue(rawValue.notNull).data)
    }

    fun testIntProperty() {
        val txn = storeTransaction
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

    fun testLongProperty() {
        val txn = storeTransaction
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

    fun testStringProperty() {
        val txn = storeTransaction
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

    fun testDoubleAndFloatProperties() {
        val txn = storeTransaction
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

    fun testDateProperty() {
        val txn = storeTransaction
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

    fun testBooleanProperty() {
        val txn = storeTransaction
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
        Assert.assertNull(entity.getProperty("ready"))
    }

    fun testHeterogeneousProperties() {
        val txn = storeTransaction
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

    @TestFor(issue = "XD-509")
    fun testComparableSetNewEmpty() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        Assert.assertFalse(entity.setProperty("subsystems", newComparableSet()))
        Assert.assertTrue(entity.propertyNames.isEmpty())
    }

    fun testComparableSetNew() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        val subsystems = newComparableSet("Search Parser", "Agile Board", "Full Text Index", "REST API", "Workflow", "Agile Board")
        entity.setProperty("subsystems", subsystems)
        txn.flush()
        val propValue = entity.getProperty("subsystems")
        Assert.assertTrue(propValue is ComparableSet<*>)
        val readSet = propValue as ComparableSet<String>
        Assert.assertFalse(readSet.isEmpty)
        Assert.assertFalse(readSet.isDirty)
        Assert.assertEquals(subsystems, propValue)
    }

    fun testComparableSetAdd() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        val subsystems = newComparableSet("Search Parser", "Agile Board")
        entity.setProperty("subsystems", subsystems)
        txn.flush()
        var propValue = entity.getProperty("subsystems")
        Assert.assertTrue(propValue is ComparableSet<*>)
        var updateSet = propValue as ComparableSet<String>
        updateSet.addItem("Obsolete Subsystem")
        Assert.assertTrue(updateSet.isDirty)
        entity.setProperty("subsystems", updateSet)
        txn.flush()
        propValue = entity.getProperty("subsystems")
        Assert.assertTrue(propValue is ComparableSet<*>)
        updateSet = propValue as ComparableSet<String>
        Assert.assertFalse(updateSet.isEmpty)
        Assert.assertFalse(updateSet.isDirty)
        Assert.assertEquals(newComparableSet("Search Parser", "Agile Board", "Obsolete Subsystem"), propValue)
    }

    fun testComparableSetAddAll() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        entity.setProperty("subsystems", newComparableSet("Search Parser", "Agile Board"))
        txn.flush()
        entity.setProperty("subsystems", newComparableSet("Search Parser", "Agile Board", "Obsolete Subsystem"))
        txn.flush()
        val propValue = entity.getProperty("subsystems")
        Assert.assertTrue(propValue is ComparableSet<*>)
        Assert.assertEquals(newComparableSet("Search Parser", "Agile Board", "Obsolete Subsystem"), propValue)
    }

    fun testComparableSetRemove() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        val subsystems = newComparableSet("Search Parser", "Agile Board")
        entity.setProperty("subsystems", subsystems)
        txn.flush()
        var propValue = entity.getProperty("subsystems")
        Assert.assertTrue(propValue is ComparableSet<*>)
        var updateSet = propValue as ComparableSet<String>
        updateSet.removeItem("Agile Board")
        Assert.assertTrue(updateSet.isDirty)
        entity.setProperty("subsystems", updateSet)
        txn.flush()
        propValue = entity.getProperty("subsystems")
        Assert.assertTrue(propValue is ComparableSet<*>)
        updateSet = propValue as ComparableSet<String>
        Assert.assertFalse(updateSet.isEmpty)
        Assert.assertFalse(updateSet.isDirty)
        Assert.assertEquals(newComparableSet("Search Parser"), propValue)
    }

    @TestFor(issue = "XD-509")
    fun testComparableSetClear() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        val subsystems = newComparableSet("Search Parser", "Agile Board")
        entity.setProperty("subsystems", subsystems)
        txn.flush()
        entity.setProperty("subsystems", newComparableSet())
        txn.flush()
        Assert.assertNull(entity.getProperty("subsystems"))
    }

    private fun newComparableSet(vararg values: String): ComparableSet<String> {
        val set = ComparableSet<String>()
        for (value in values) {
            set.addItem(value)
        }
        return set
    }

    fun testOverwriteProperty() {
        val txn = storeTransaction
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

    fun testDeleteProperty() {
        val txn = storeTransaction
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

    @Throws(Exception::class)
    fun testReadingWithoutTransaction() {
        var txn = storeTransaction
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
        reinit()
        txn = storeTransaction
        for (issue in txn.getAll("Issue")) {
            Assert.assertEquals("my name", issue.getProperty("name"))
            val users: Iterable<Entity> = issue.getLinks("creator")
            for (user in users) {
                Assert.assertEquals("charisma user", user.getProperty("name"))
            }
        }
    }

    fun testClearingProperties() {
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        issue.setProperty("description", "This is a test issue")
        issue.setProperty("size", 0)
        issue.setProperty("rank", 0.5)
        txn.flush()
        Assert.assertNotNull(issue.getProperty("description"))
        Assert.assertNotNull(issue.getProperty("size"))
        Assert.assertNotNull(issue.getProperty("rank"))
        entityStore.clearProperties(txn, issue)
        txn.flush()
        Assert.assertNull(issue.getProperty("description"))
        Assert.assertNull(issue.getProperty("size"))
        Assert.assertNull(issue.getProperty("rank"))
    }

    fun testDeleteEntities() {
        val txn = storeTransaction
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

    fun testRenameEntityType() {
        val txn = storeTransaction
        for (i in 0..9) {
            txn.newEntity("Issue")
        }
        txn.flush()
        Assert.assertEquals(10, txn.getAll("Issue").size())
        entityStore.renameEntityType("Issue", "Comment")
        txn.flush()
        Assert.assertEquals(0, txn.getAll("Issue").size())
        Assert.assertEquals(10, txn.getAll("Comment").size())
    }

    fun testRenameNonExistingEntityType() {
        val txn = storeTransaction
        for (i in 0..9) {
            txn.newEntity("Issue")
        }
        txn.flush()
        Assert.assertEquals(10, txn.getAll("Issue").size())
        TestUtil.runWithExpectedException({ entityStore.renameEntityType("Comment", "Issue") }, IllegalArgumentException::class.java)
    }

    @Throws(InterruptedException::class)
    fun testConcurrentSerializableChanges() {
        val e = entityStore.computeInTransaction { txn ->
            txn.newEntity("E")
        }
        val count = 100
        val target = Runnable {
            val txn = entityStore.beginTransaction()
            try {
                for (i in 0..count) {
                    do {
                        e.setProperty("i", i)
                        e.setProperty("s", i.toString())
                    } while (!txn.flush())
                }
            } finally {
                txn.abort()
            }
        }
        val t1 = Thread(target)
        val t2 = Thread(target)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        entityStore.executeInReadonlyTransaction {
            Assert.assertEquals(count, e.getProperty("i"))
            Assert.assertEquals(count.toString(), e.getProperty("s"))
        }
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

    fun testAsciiUTFDecodingBenchmark() {
        val s = "This is sample ASCII string of not that great size, but large enough to use in the benchmark"
        TestUtil.time("Constructing string from data input") {
            try {
                val out = LightByteArrayOutputStream()
                val output = DataOutputStream(out)
                output.writeUTF(s)
                val stream: InputStream = ByteArraySizedInputStream(out.toByteArray(), 0, out.size())
                stream.mark(Int.MAX_VALUE)
                for (i in 0..9999999) {
                    stream.reset()
                    TestCase.assertEquals(s, DataInputStream(stream).readUTF())
                }
            } catch (e: IOException) {
                throw ExodusException.toEntityStoreException(e)
            }
        }
        TestUtil.time("Constructing strings from bytes") {
            val bytes = s.toByteArray()
            for (i in 0..9999999) {
                TestCase.assertEquals(s, UTFUtil.fromAsciiByteArray(bytes, 0, bytes.size))
            }
        }
    }

    fun testTxnCachesIsolation() {
        val issue = entityStore.computeInTransaction { txn ->
            txn.newEntity("Issue").apply { setProperty("description", "1") }
        }
        val txn = storeTransaction
        txn.revert()
        Assert.assertEquals("1", issue.getProperty("description"))
        entityStore.executeInTransaction {
            issue.setProperty("description", "2")
        }
        txn.revert()
        Assert.assertEquals("2", issue.getProperty("description"))
    }

    fun testTxnCachesIsolation2() {
        val issue = entityStore.computeInTransaction { txn ->
            txn.newEntity("Issue").apply { setProperty("description", "1") }
        }
        val txn = storeTransaction
        txn.revert()
        Assert.assertEquals("1", issue.getProperty("description"))
        issue.setProperty("description", "2")
        entityStore.executeInTransaction {
            issue.setProperty("description", "3")
        }
        Assert.assertFalse(txn.flush())
        Assert.assertEquals("3", issue.getProperty("description"))
    }

    @TestFor(issue = "XD-530")
    fun testEntityStoreClear() {
        val store = entityStore
        val user = store.computeInTransaction { txn ->
            txn.newEntity("User").apply { setProperty("login", "penemue") }
        }
        store.executeInReadonlyTransaction {
            Assert.assertEquals("penemue", user.getProperty("login"))
        }
        store.clear()
        store.executeInReadonlyTransaction {
            Assert.assertNull(user.getProperty("login"))
        }
        store.executeInTransaction { txn ->
            txn.newEntity("UserProfile")
        }
        store.executeInTransaction { txn ->
            txn.getSequence("qwerty").increment()
        }
    }

    @TestFor(issue = "XD-810")
    fun testTooBigProperty() {
        val dir = initTempFolder()
        try {
            PersistentEntityStores.newInstance(Environments.newInstance(dir, newEnvironmentConfig {
                isLogCacheShared = false
                logCachePageSize = 1024
                logFileSize = 1
            })).use { store ->
                store.environment.use {
                    val issue = store.computeInTransaction { txn ->
                        txn.newEntity("Issue").apply {
                            setProperty("p1", "")
                            setProperty("p2", "")
                        }
                    }
                    val wasTooBig = try {
                        store.executeInTransaction { txn ->
                            issue.setProperty("p1", "value")
                            issue.setProperty("p2", "value")
                            txn.flush()
                            issue.setProperty("p2", "".padEnd(1024))
                        }
                        false
                    } catch (_: TooBigLoggableException) {
                        true
                    }
                    Assert.assertTrue(wasTooBig)
                }
            }
        } finally {
            cleanUp(dir)
        }
    }

    fun testSetPhantomLink() {
        setOrAddPhantomLink(false)
    }

    fun testAddPhantomLink() {
        setOrAddPhantomLink(true)
    }

    private fun setOrAddPhantomLink(setLink: Boolean) {
        val store = entityStore
        store.environment.environmentConfig.isGcEnabled = false
        store.config.isDebugTestLinkedEntities = true
        val issue = store.computeInTransaction { txn ->
            txn.newEntity("Issue")
        }
        val comment = store.computeInTransaction { txn ->
            txn.newEntity("Comment")
        }
        val startBoth = CountDownLatch(2)
        val deleted = Semaphore(0)
        DeferredIO.getJobProcessor().queue(object : Job() {
            override fun execute() {
                store.executeInTransaction { txn ->
                    startBoth.countDown()
                    try {
                        startBoth.await()
                    } catch (ignore: InterruptedException) {
                    }
                    comment.delete()
                    txn.flush()
                    deleted.release()
                }
            }
        })
        val i = intArrayOf(0)
        TestUtil.runWithExpectedException({
            store.executeInTransaction {
                val first = i[0] == 0
                if (first) {
                    startBoth.countDown()
                    try {
                        startBoth.await()
                    } catch (ignore: InterruptedException) {
                    }
                }
                ++i[0]
                if (setLink) {
                    issue.setLink("comment", comment)
                } else {
                    issue.addLink("comment", comment)
                }
                if (first) {
                    deleted.acquireUninterruptibly()
                }
            }
        }, PhantomLinkException::class.java)
        Assert.assertEquals(2, i[0])
        store.executeInReadonlyTransaction {
            Assert.assertNull(issue.getLink("comment"))
        }
    }
}
