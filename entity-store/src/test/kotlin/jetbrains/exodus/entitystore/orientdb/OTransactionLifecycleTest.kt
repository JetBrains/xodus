package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.exception.ODatabaseException
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.orientechnologies.orient.core.tx.OTransaction
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import junit.framework.TestCase
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class OTransactionLifecycleTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB(true)

    override val orientDb = orientDbRule

    @Test
    fun `open, begin, commit, close - no changes`() {
        val session = orientDb.openSession()

        TestCase.assertFalse(session.transaction == null)
        session.begin()

        TestCase.assertTrue(session.transaction.status == OTransaction.TXSTATUS.BEGUN)

        session.commit()

        TestCase.assertTrue(session.transaction.status == OTransaction.TXSTATUS.INVALID)
        TestCase.assertFalse(session.hasActiveTransaction())
        TestCase.assertTrue(session.isActiveOnCurrentThread)

        session.close()
        TestCase.assertFalse(session.isActiveOnCurrentThread)
        TestCase.assertTrue(session.isClosed)
    }

    @Test
    fun `open, begin, commit, close - changes`() {
        val session = orientDb.openSession()
        val oClass = session.getOrCreateVertexClass("trista")

        TestCase.assertFalse(session.transaction == null)

        session.begin()

        TestCase.assertTrue(session.transaction.status == OTransaction.TXSTATUS.BEGUN)

        val trista = session.newVertex(oClass)
        trista.setProperty("name", "opca trista")
        trista.save<OVertex>()
        session.commit()

        TestCase.assertTrue(session.transaction.status == OTransaction.TXSTATUS.INVALID)
        TestCase.assertFalse(session.hasActiveTransaction())
        TestCase.assertTrue(session.isActiveOnCurrentThread)

        session.begin()
        TestCase.assertNotNull(session.getRecord(trista.identity))
        session.commit()

        session.close()
        TestCase.assertFalse(session.isActiveOnCurrentThread)
        TestCase.assertTrue(session.isClosed)
    }

    @Test
    fun `open, begin, rollback, close - no changes`() {
        val session = orientDb.openSession()

        TestCase.assertFalse(session.transaction == null)

        session.begin()

        TestCase.assertTrue(session.transaction.status == OTransaction.TXSTATUS.BEGUN)

        session.rollback()

        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)
        TestCase.assertFalse(session.hasActiveTransaction())
        TestCase.assertTrue(session.isActiveOnCurrentThread)

        session.close()
        TestCase.assertFalse(session.isActiveOnCurrentThread)
        TestCase.assertTrue(session.isClosed)
    }

    @Test
    fun `open, begin, rollback, close - changes`() {
        val session = orientDb.openSession()
        val oClass = session.getOrCreateVertexClass("trista")

        TestCase.assertFalse(session.transaction == null)

        session.begin()

        TestCase.assertTrue(session.transaction.status == OTransaction.TXSTATUS.BEGUN)

        val trista = session.newVertex(oClass)
        trista.setProperty("name", "opca trista")
        trista.save<OVertex>()
        session.rollback()

        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)
        TestCase.assertFalse(session.hasActiveTransaction())
        TestCase.assertTrue(session.isActiveOnCurrentThread)

        session.begin()
        TestCase.assertNull(session.getRecord(trista.identity))
        session.commit()

        session.close()
        TestCase.assertFalse(session.isActiveOnCurrentThread)
        TestCase.assertTrue(session.isClosed)
    }

    @Test
    fun `commit() throws an exception if there is no active transaction`() {
        val session = orientDb.openSession()

        TestCase.assertFalse(session.hasActiveTransaction())
        assertFailsWith<ODatabaseException> { session.commit() }

        session.begin()

        TestCase.assertTrue(session.hasActiveTransaction())

        session.commit()

        TestCase.assertFalse(session.hasActiveTransaction())
        TestCase.assertTrue(session.isActiveOnCurrentThread)

        assertFailsWith<ODatabaseException> { session.commit() }

        TestCase.assertTrue(session.isActiveOnCurrentThread)

        // the session is still usable
        session.begin()
        session.commit()

        session.close()
        TestCase.assertFalse(session.isActiveOnCurrentThread)
        TestCase.assertTrue(session.isClosed)
    }

    @Test
    fun `rollback() does NOT throw an exception if there is no active transaction`() {
        val session = orientDb.openSession()

        TestCase.assertFalse(session.hasActiveTransaction())

        session.begin()

        TestCase.assertTrue(session.hasActiveTransaction())

        session.commit()

        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)
        TestCase.assertFalse(session.hasActiveTransaction())
        TestCase.assertTrue(session.isActiveOnCurrentThread)

        // rollback() does not throw an exception if there is no active transaction, but commit() throws
        session.rollback()
        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)

        session.close()
        TestCase.assertFalse(session.isActiveOnCurrentThread)
        TestCase.assertTrue(session.isClosed)
    }

    @Test
    fun `if commit() fails, changes get rolled back`() {
        val session = orientDb.openSession()
        val oClass = session.getOrCreateVertexClass("trista")
        oClass.createProperty("name", OType.STRING)
        oClass.createIndex("idx_name", OClass.INDEX_TYPE.UNIQUE, "name")

        session.begin() // tx1
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")
        trista1.save<OVertex>()
        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "dvesti")
        trista2.save<OVertex>()

        // if commit
        assertFailsWith<ORecordDuplicatedException> { session.commit() }

        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)
        TestCase.assertFalse(session.hasActiveTransaction())
        TestCase.assertTrue(session.isActiveOnCurrentThread)

        session.begin()
        TestCase.assertNull(session.getRecord<OVertex>(trista1.identity))
        TestCase.assertNull(session.getRecord<OVertex>(trista2.identity))
        session.commit()
        session.close()
    }

    @Test
    fun `embedded transactions successful case`() {
        val session = orientDb.openSession()
        session.getOrCreateVertexClass("trista")
        assertEquals(0, session.transaction.amountOfNestedTxs())

        session.begin() // tx1
        assertEquals(OTransaction.TXSTATUS.BEGUN, session.transaction.status)
        assertEquals(1, session.transaction.amountOfNestedTxs())
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")
        trista1.save<OVertex>()

        session.begin() // tx2
        assertEquals(OTransaction.TXSTATUS.BEGUN, session.transaction.status)
        assertEquals(2, session.transaction.amountOfNestedTxs())
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "sth")
        trista2.save<OVertex>()

        session.commit()

        assertEquals(OTransaction.TXSTATUS.BEGUN, session.transaction.status)
        assertEquals(1, session.transaction.amountOfNestedTxs())
        TestCase.assertTrue(session.hasActiveTransaction())

        session.commit()

        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)
        assertEquals(0, session.transaction.amountOfNestedTxs())
        TestCase.assertFalse(session.hasActiveTransaction())

        session.begin()
        TestCase.assertNotNull(session.getRecord<OVertex>(trista1.identity))
        TestCase.assertNotNull(session.getRecord<OVertex>(trista2.identity))
        session.commit()
        session.close()
    }

    @Test
    fun `rollback(force = true) on an embedded transaction rolls back all the transactions`() {
        val session = orientDb.openSession()
        session.getOrCreateVertexClass("trista")

        session.begin() // tx1
        assertEquals(OTransaction.TXSTATUS.BEGUN, session.transaction.status)
        assertEquals(1, session.transaction.amountOfNestedTxs())
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")
        trista1.save<OVertex>()

        session.begin() // tx2
        assertEquals(OTransaction.TXSTATUS.BEGUN, session.transaction.status)
        assertEquals(2, session.transaction.amountOfNestedTxs())
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "sth")
        trista2.save<OVertex>()

        session.rollback(true)

        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)
        assertEquals(0, session.transaction.amountOfNestedTxs())

        session.begin()
        TestCase.assertNull(session.getRecord<OVertex>(trista1.identity))
        TestCase.assertNull(session.getRecord<OVertex>(trista2.identity))
        session.commit()
        session.close()
    }

    @Test
    fun `rollback() on an embedded transaction decreases amountOfNestedTxs by 1`() {
        val session = orientDb.openSession()
        session.getOrCreateVertexClass("trista")

        session.begin() // tx1
        assertEquals(OTransaction.TXSTATUS.BEGUN, session.transaction.status)
        assertEquals(1, session.transaction.amountOfNestedTxs())
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")
        trista1.save<OVertex>()

        session.begin() // tx2
        assertEquals(OTransaction.TXSTATUS.BEGUN, session.transaction.status)
        assertEquals(2, session.transaction.amountOfNestedTxs())
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "sth")
        trista2.save<OVertex>()

        session.rollback()

        assertEquals(OTransaction.TXSTATUS.ROLLBACKING, session.transaction.status)
        assertEquals(1, session.transaction.amountOfNestedTxs())

        session.rollback()

        assertEquals(OTransaction.TXSTATUS.INVALID, session.transaction.status)
        assertEquals(0, session.transaction.amountOfNestedTxs())

        session.begin()
        TestCase.assertNull(session.getRecord<OVertex>(trista1.identity))
        TestCase.assertNull(session.getRecord<OVertex>(trista2.identity))
        session.commit()
        session.close()
    }

    @Test
    fun `commit() on an embedded transaction does not validate constraints, only top level transaction validates`() {
        val session = orientDb.openSession()
        val oClass = session.getOrCreateVertexClass("trista")
        oClass.createProperty("name", OType.STRING)
        oClass.createIndex("idx_name", OClass.INDEX_TYPE.UNIQUE, "name")

        session.begin() // tx1
        TestCase.assertTrue(session.hasActiveTransaction())

        session.begin() // tx2
        TestCase.assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")
        trista1.save<OVertex>()
        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "dvesti")
        trista2.save<OVertex>()

        session.commit() // tx2
        assertFailsWith<ORecordDuplicatedException> { session.commit() } // tx1

        TestCase.assertFalse(session.hasActiveTransaction())

        session.begin()
        TestCase.assertNull(session.getRecord<OVertex>(trista1.identity))
        TestCase.assertNull(session.getRecord<OVertex>(trista2.identity))
        session.commit()
        session.close()
    }
}