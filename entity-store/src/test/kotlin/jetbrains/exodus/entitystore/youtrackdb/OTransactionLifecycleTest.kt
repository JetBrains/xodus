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
package jetbrains.exodus.entitystore.youtrackdb

import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.exception.DatabaseException
import com.jetbrains.youtrack.db.api.exception.ModificationOperationProhibitedException
import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException
import com.jetbrains.youtrack.db.api.record.DBRecord
import com.jetbrains.youtrack.db.api.record.Vertex
import com.jetbrains.youtrack.db.api.schema.PropertyType
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction.TXSTATUS
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionNoTx
import jetbrains.exodus.entitystore.youtrackdb.testutil.InMemoryYouTrackDB
import jetbrains.exodus.entitystore.youtrackdb.testutil.OTestMixin
import junit.framework.TestCase.assertFalse
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import kotlin.test.*

class OTransactionLifecycleTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryYouTrackDB(true)

    override val youTrackDb = orientDbRule

    @Test
    fun `session-freeze(true) makes the session read-only`() {
        val session = youTrackDb.openSession()
        session.freeze(true)
        val tx = session.begin()

        val v = tx.newVertex()
        assertFailsWith<ModificationOperationProhibitedException> { tx.commit() }

        // after release() we can write again
        session.release()

        val tx2 = session.begin()
        val v2 = tx2.newVertex()
        tx2.commit()

        session.close()
    }

    @Test
    fun `open, begin, commit, close - no changes`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal

        assertFalse(session.transactionInternal == null)
        assertEquals(DatabaseSession.STATUS.OPEN, session.status)
        session.begin()

        assertEquals(session.transactionInternal.status, TXSTATUS.BEGUN)

        session.commit()

        assertEquals(session.transactionInternal.status, TXSTATUS.INVALID)
        assertEquals(DatabaseSession.STATUS.OPEN, session.status)
        assertFalse(session.hasActiveTransaction())
        assertTrue(session.isActiveOnCurrentThread)

        session.close()
        assertEquals(DatabaseSession.STATUS.CLOSED, session.status)
        assertFalse(session.isActiveOnCurrentThread)
        assertTrue(session.isClosed)
    }

    @Test
    fun `open, begin, commit, close - changes`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal
        val oClass = session.getOrCreateVertexClass("trista")

        assertFalse(session.transactionInternal == null)

        session.begin()

        assertTrue(session.transactionInternal.status == TXSTATUS.BEGUN)

        val trista = session.newVertex(oClass)
        trista.setProperty("name", "opca trista")
        session.commit()

        assertTrue(session.transactionInternal.status == TXSTATUS.INVALID)
        assertFalse(session.hasActiveTransaction())
        assertTrue(session.isActiveOnCurrentThread)

        session.begin()
        session.load<DBRecord>(trista.identity)
        session.commit()

        session.close()
        assertFalse(session.isActiveOnCurrentThread)
        assertTrue(session.isClosed)
    }

    @Test
    fun `open, begin, rollback, close - no changes`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal

        assertFalse(session.transactionInternal == null)

        session.begin()

        assertEquals(session.transactionInternal.status, TXSTATUS.BEGUN)

        session.rollback()

        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertFalse(session.hasActiveTransaction())
        assertTrue(session.isActiveOnCurrentThread)

        session.close()
        assertFalse(session.isActiveOnCurrentThread)
        assertTrue(session.isClosed)
    }

    @Test
    fun `open, begin, rollback, close - changes`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal
        val oClass = session.getOrCreateVertexClass("trista")

        assertFalse(session.transactionInternal == null)

        session.begin()

        assertEquals(session.transactionInternal.status, TXSTATUS.BEGUN)

        val trista = session.newVertex(oClass)
        trista.setProperty("name", "opca trista")
        session.rollback()

        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertFalse(session.hasActiveTransaction())
        assertTrue(session.isActiveOnCurrentThread)

        session.begin()
        try {
            session.load<DBRecord>(trista.identity)
            Assert.fail()
        } catch (e: RecordNotFoundException) {
            // expected
        }

        session.commit()

        session.close()
        assertFalse(session.isActiveOnCurrentThread)
        assertTrue(session.isClosed)
    }

    @Test
    fun `commit() throws an exception if there is no active transaction`() {
        val session: DatabaseSessionInternal = youTrackDb.openSession() as DatabaseSessionInternal

        assertFalse(session.hasActiveTransaction())
        assertFailsWith<DatabaseException> { session.commit() }

        session.begin()

        assertTrue(session.hasActiveTransaction())

        session.commit()

        assertFalse(session.hasActiveTransaction())
        assertTrue(session.isActiveOnCurrentThread)

        assertFailsWith<DatabaseException> { session.commit() }

        assertTrue(session.isActiveOnCurrentThread)
        assertEquals(DatabaseSession.STATUS.OPEN, session.status)

        // the session is still usable
        session.begin()
        session.commit()

        session.close()
        assertFalse(session.isActiveOnCurrentThread)
        assertTrue(session.isClosed)
    }

    @Test
    fun `rollback() does NOT throw an exception if there is no active transaction`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal

        assertFalse(session.hasActiveTransaction())

        session.begin()

        assertTrue(session.hasActiveTransaction())

        session.commit()

        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertFalse(session.hasActiveTransaction())
        assertTrue(session.isActiveOnCurrentThread)

        // rollback() does not throw an exception if there is no active transaction, but commit() throws
        session.rollback()
        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)

        session.close()
        assertFalse(session.isActiveOnCurrentThread)
        assertTrue(session.isClosed)
    }

    @Test
    fun `if commit() fails, changes get rolled back`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal
        val oClass = session.getOrCreateVertexClass("trista")
        oClass.createProperty("name", PropertyType.STRING)
        oClass.createIndex("idx_name", SchemaClass.INDEX_TYPE.UNIQUE, "name")

        session.begin() // tx1
        assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")
        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "dvesti")

        // if commit
        assertFailsWith<RecordDuplicatedException> { session.commit() }

        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertFalse(session.hasActiveTransaction())
        assertTrue(session.isActiveOnCurrentThread)

        session.begin()
        try {
            session.load<DBRecord>(trista1.identity)
            Assert.fail()
        } catch (_: RecordNotFoundException) {

        }

        try {
            session.load<DBRecord>(trista2.identity)
            Assert.fail()
        } catch (_: RecordNotFoundException) {
        }
        session.commit()
        session.close()
    }

    @Test
    fun `embedded transactions successful case`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal
        session.getOrCreateVertexClass("trista")
        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertEquals(0, session.transactionInternal.amountOfNestedTxs())

        session.begin() // tx1
        val tx1 = session.transactionInternal
        assertEquals(TXSTATUS.BEGUN, session.transactionInternal.status)
        assertEquals(1, session.transactionInternal.amountOfNestedTxs())
        assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")

        session.begin() // tx2
        val tx2 = session.transactionInternal
        // Orient does not a separate transaction instance for an embedded transaction
        assertEquals(tx1, tx2)
        assertEquals(TXSTATUS.BEGUN, session.transactionInternal.status)
        assertEquals(2, session.transactionInternal.amountOfNestedTxs())
        assertTrue(session.hasActiveTransaction())

        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "sth")

        session.commit() // tx2

        assertEquals(tx1, session.transactionInternal)
        assertEquals(TXSTATUS.BEGUN, session.transactionInternal.status)
        assertEquals(1, session.transactionInternal.amountOfNestedTxs())
        assertTrue(session.hasActiveTransaction())

        session.commit() // tx1

        assertNotEquals(tx1, session.transactionInternal)
        assertIs<FrontendTransactionNoTx>(session.transactionInternal)
        assertEquals(TXSTATUS.COMPLETED, tx1.status)
        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertEquals(0, session.transactionInternal.amountOfNestedTxs())
        assertFalse(session.hasActiveTransaction())

        session.begin()
        assertNotEquals(tx1, session.transactionInternal)
        session.load<Vertex>(trista1.identity)
        session.load<Vertex>(trista2.identity)
        session.commit()
        session.close()
    }

    @Test
    fun `rollback(force = true) on an embedded transaction rolls back all the transactions`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal
        session.getOrCreateVertexClass("trista")

        session.begin() // tx1
        assertEquals(TXSTATUS.BEGUN, session.transactionInternal.status)
        assertEquals(1, session.transactionInternal.amountOfNestedTxs())
        assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")

        session.begin() // tx2
        assertEquals(TXSTATUS.BEGUN, session.transactionInternal.status)
        assertEquals(2, session.transactionInternal.amountOfNestedTxs())
        assertTrue(session.hasActiveTransaction())

        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "sth")

        session.rollback(true)

        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertEquals(0, session.transactionInternal.amountOfNestedTxs())

        session.begin()
        try {
            session.loadVertex(trista1.identity)
            Assert.fail()
        } catch (e: RecordNotFoundException) {
            // expected
        }
        try {
            session.loadVertex(trista2.identity)
            Assert.fail()
        } catch (e: RecordNotFoundException) {
            // expected
        }
        session.commit()
        session.close()
    }

    @Test
    fun `rollback() on an embedded transaction decreases amountOfNestedTxs by 1`() {
        val session = youTrackDb.openSession() as DatabaseSessionInternal
        session.getOrCreateVertexClass("trista")

        session.begin() // tx1
        assertEquals(TXSTATUS.BEGUN, session.transactionInternal.status)
        assertEquals(1, session.transactionInternal.amountOfNestedTxs())
        assertTrue(session.hasActiveTransaction())

        val trista1 = session.newVertex("trista")
        trista1.setProperty("name", "dvesti")

        session.begin() // tx2
        assertEquals(TXSTATUS.BEGUN, session.transactionInternal.status)
        assertEquals(2, session.transactionInternal.amountOfNestedTxs())
        assertTrue(session.hasActiveTransaction())

        val trista2 = session.newVertex("trista")
        trista2.setProperty("name", "sth")

        session.rollback()

        assertEquals(TXSTATUS.ROLLBACKING, session.transactionInternal.status)
        assertEquals(1, session.transactionInternal.amountOfNestedTxs())

        session.rollback()

        assertEquals(TXSTATUS.INVALID, session.transactionInternal.status)
        assertEquals(0, session.transactionInternal.amountOfNestedTxs())

        session.begin()
        try {
            session.loadVertex(trista1.identity)
            Assert.fail()
        } catch (e: RecordNotFoundException) {
            // expected
        }
        try {
            session.loadVertex(trista2.identity)
            Assert.fail()
        } catch (e: RecordNotFoundException) {
            // expected
        }
        session.commit()
        session.close()
    }

    @Test
    fun `commit() on an embedded transaction does not validate constraints, only top level transaction validates`() {
        val session = youTrackDb.openSession()
        val oClass = session.getOrCreateVertexClass("trista")
        oClass.createProperty("name", PropertyType.STRING)
        oClass.createIndex("idx_name", SchemaClass.INDEX_TYPE.UNIQUE, "name")

        val tx1 = session.begin() // tx1
        assertTrue(session.hasActiveTransaction())

        val tx2 = session.begin() // tx2
        assertTrue(session.hasActiveTransaction())

        val trista1 = tx2.newVertex("trista")
        trista1.setProperty("name", "dvesti")
        val trista2 = tx2.newVertex("trista")
        trista2.setProperty("name", "dvesti")

        tx2.commit() // tx2
        assertFailsWith<RecordDuplicatedException> { tx1.commit() } // tx1

        assertFalse(session.hasActiveTransaction())

        val tx = session.begin()
        try {
            tx.load<DBRecord>(trista1.identity)
            Assert.fail()
        } catch (e: RecordNotFoundException) {
            // expected
        }

        try {
            tx.load<DBRecord>(trista2.identity)
            Assert.fail()
        } catch (e: RecordNotFoundException) {
            // expected
        }
        tx.commit()
        session.close()
    }
}