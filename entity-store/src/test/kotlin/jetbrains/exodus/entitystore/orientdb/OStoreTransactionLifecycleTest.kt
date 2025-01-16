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

import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException
import com.jetbrains.youtrack.db.api.schema.PropertyType
import com.jetbrains.youtrack.db.api.schema.SchemaClass
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryYouTrackDB
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
    val orientDbRule = InMemoryYouTrackDB(true)

    override val youTrackDb = orientDbRule

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
        commitAbort.forEach { (terminalTxActionName, terminalTxAction) ->
            allTxActions.forEach { (txActionName, txAction) ->
                val tx = beginTransaction()
                assertFalse(tx.isFinished)

                terminalTxAction(tx)
                assertTrue(tx.isFinished)
                assertFailsWith<IllegalStateException>("$txActionName after $terminalTxActionName must lead to an IllegalStateException") {
                    txAction(
                        tx
                    )
                }
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
                assertFalse(
                    "$notTerminalTxActionName finished the transaction but it should not have",
                    tx.isFinished
                )

                txAction(tx)
                if (!tx.isFinished) {
                    tx.commit()
                }
            }
        }
    }

    @Test
    fun `commit() and flush() finish the transaction if error happens`() {
        val session = youTrackDb.openSession()
        val oClass = session.getOrCreateVertexClass("trista")
        oClass.createProperty(session, "name", PropertyType.STRING)
        oClass.createIndex(session, "idx_name", SchemaClass.INDEX_TYPE.UNIQUE, "name")
        session.close()


        allTxActions.forEach { (actionName, txAction) ->
            val tx = beginTransaction()


            val trista1 = tx.databaseSession.newVertex("trista")
            trista1.setProperty("name", "dvesti")
            trista1.save()
            val trista2 = tx.databaseSession.newVertex("trista")
            trista2.setProperty("name", "dvesti")
            trista2.save()

            when (actionName) {
                "commit",
                "flush" -> assertFailsWith<RecordDuplicatedException> { txAction(tx) }

                "revert" -> {
                    txAction(tx)
                    tx.abort()
                }

                "abort" -> txAction(tx)
            }
        }

    }
}