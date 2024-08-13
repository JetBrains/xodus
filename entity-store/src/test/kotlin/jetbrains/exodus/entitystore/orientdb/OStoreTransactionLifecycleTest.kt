package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import junit.framework.TestCase.assertFalse
import junit.framework.TestCase.assertTrue
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

private typealias TxAction = (StoreTransaction) -> Unit

class OStoreTransactionLifecycleTest : OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB(true)

    override val orientDb = orientDbRule

    private val allTxActions: Map<String, TxAction> = mapOf(
        "commit" to { tx -> tx.commit() },
        "flush" to { tx -> tx.flush() },
        "revert" to { tx -> tx.revert() },
        "abort" to { tx -> tx.abort() }
    )

    private val commitAbort: Map<String, TxAction> = mapOf(
        "commit" to { tx -> tx.commit() },
        "abort" to { tx -> tx.abort() }
    )

    private val flushRevert: Map<String, TxAction> = mapOf(
        "flush" to { tx -> tx.flush() },
        "revert" to { tx -> tx.revert() },
    )

    @Test
    fun `commit() and abort() finish transaction`() {
        orientDb.store.executeInTransaction {  }
        commitAbort.forEach { (terminalTxActionName, terminalTxAction) ->
            allTxActions.forEach { (txActionName, txAction) ->
            val tx = beginTransaction()
            assertFalse(tx.isFinished)

            terminalTxAction(tx)
            assertTrue(tx.isFinished)
                assertFailsWith<IllegalStateException>("$txActionName after $terminalTxActionName must lead to an IllegalStateException") { txAction(tx) }
            }
        }
    }

    @Test
    fun `flush() and revert() do not finish transaction`() {
        flushRevert.forEach { (notTerminalTxActionName, notTerminalTxAction) ->
            allTxActions.forEach { (_, txAction) ->
                val tx = beginTransaction()
                assertFalse(tx.isFinished)

                notTerminalTxAction(tx)
                assertFalse("$notTerminalTxActionName finished the transaction but it should not have", tx.isFinished)

                txAction(tx)
                if (!tx.isFinished) {
                    tx.commit()
                }
            }
        }
    }

    @Test
    fun `commit() and flush() finish the transaction if error happens`() {
        val session = orientDb.openSession()
        val oClass = session.getOrCreateVertexClass("trista")
        oClass.createProperty("name", OType.STRING)
        oClass.createIndex("idx_name", OClass.INDEX_TYPE.UNIQUE, "name")
        session.close()

        allTxActions.forEach { (actionName, txAction) ->
            val tx = beginTransaction()

            val trista1 = session.newVertex("trista")
            trista1.setProperty("name", "dvesti")
            trista1.save<OVertex>()
            val trista2 = session.newVertex("trista")
            trista2.setProperty("name", "dvesti")
            trista2.save<OVertex>()

            when (actionName) {
                "commit",
                "flush" -> assertFailsWith<ORecordDuplicatedException> { txAction(tx) }

                "revert" -> {
                    txAction(tx)
                    tx.abort()
                }
                "abort" -> txAction(tx)
            }
        }
    }
}