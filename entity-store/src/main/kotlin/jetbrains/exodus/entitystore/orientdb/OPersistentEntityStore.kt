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

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.management.Statistics
import java.io.File
import java.util.concurrent.Executor
import java.util.concurrent.Executors

class OPersistentEntityStore(
    val databaseProvider: ODatabaseProvider,
    private val name: String,
    override val countExecutor: Executor = Executors.newSingleThreadExecutor(),
    private val schemaBuddy: OSchemaBuddy = OSchemaBuddyImpl(databaseProvider)
) : PersistentEntityStore, OEntityStore {

    private val config = PersistentEntityStoreConfig()
    private val dummyJobProcessor = object : MultiThreadDelegatingJobProcessor("dummy", 1) {}
    private val dummyStatistics = object : Statistics<Enum<*>>(arrayOf()) {}
    private val env = OEnvironment(databaseProvider.database, this)
    private val currentTransaction = ThreadLocal<OStoreTransactionImpl>()

    override fun close() {
        //or it should be closed independently
        currentTransaction.get()?.abort()

        currentTransaction.remove()
        databaseProvider.close()
    }

    override fun getName() = name

    override fun getLocation(): String {
        return databaseProvider.databaseLocation
    }

    override fun beginTransaction(): StoreTransaction {
        return beginTransactionImpl(readOnly = false)
    }

    override fun beginExclusiveTransaction(): StoreTransaction {
        return beginTransactionImpl(readOnly = false)
    }

    override fun beginReadonlyTransaction(): StoreTransaction {
        return beginTransactionImpl(readOnly = true)
    }

    private fun beginTransactionImpl(readOnly: Boolean): StoreTransaction {
        var currentTx: OStoreTransactionImpl? = currentTransaction.get()
        check(currentTx == null) { "EntityStore has a transaction on the current thread. Finish it before starting a new one." }

        val session = databaseProvider.acquireSession()

        currentTx = OStoreTransactionImpl(
            session,
            store = this,
            schemaBuddy,
            executeInASeparateSession = ::executeInASeparateSession,
            onFinished = ::onTransactionFinished,
            readOnly = readOnly
        )
        currentTransaction.set(currentTx)
        currentTx.begin()

        return currentTx
    }

    private fun onTransactionFinished(session: ODatabaseSession, tx: OStoreTransaction) {
        check(currentTransaction.get() == tx) { "The current transaction at EntityStore is different for one that just has finished. It must not happen." }
        check(!session.isClosed) { "The session should not be closed at this point." }
        currentTransaction.remove()
        session.close()
    }

    private fun executeInASeparateSession(action: ODatabaseSession.() -> Unit) {
        val currentSession = ODatabaseSession.getActiveSession() as ODatabaseSession?
        try {
            databaseProvider.acquireSession().use { session ->
                session.action()
            }
        } finally {
            if (currentSession != null) {
                // the previous session does not get activated on the current thread by default
                assert(!currentSession.isActiveOnCurrentThread)
                currentSession.activateOnCurrentThread()
            }
        }
    }

    override fun getCurrentTransaction(): StoreTransaction? {
        return currentTransaction.get()
    }

    override fun getBackupStrategy(): BackupStrategy {
        return object : BackupStrategy() {}
    }

    override fun getEnvironment(): OEnvironment {
        return env
    }

    override fun clear() {
        throw IllegalStateException("Should not ever be called")
    }

    override fun executeInTransaction(executable: StoreTransactionalExecutable) {
        //i'm not sure about implementation
        val txn = beginTransaction() as OStoreTransactionImpl
        try {
            executable.execute(txn)
            txn.commit()
        } catch (e: Exception) {
            txn.abort()
            throw e
        }
    }

    override fun executeInExclusiveTransaction(executable: StoreTransactionalExecutable) =
        executeInTransaction(executable)

    override fun executeInReadonlyTransaction(executable: StoreTransactionalExecutable) =
        executeInTransaction(executable)

    override fun <T : Any?> computeInTransaction(computable: StoreTransactionalComputable<T>): T {
        //i'm not sure about implementation
        val txn = beginTransaction() as OStoreTransactionImpl
        try {
            val result = computable.compute(txn)
            txn.commit()
            return result
        } catch (e: Exception) {
            txn.abort()
            throw e
        }
    }

    override fun <T : Any?> computeInExclusiveTransaction(computable: StoreTransactionalComputable<T>) =
        computeInTransaction(computable)

    override fun <T : Any?> computeInReadonlyTransaction(computable: StoreTransactionalComputable<T>) =
        computeInTransaction(computable)

    override fun getBlobVault() = DummyBlobVault(config)

    override fun registerCustomPropertyType(
        txn: StoreTransaction,
        clazz: Class<out Comparable<Any?>>,
        binding: ComparableBinding
    ) {
        throw NotImplementedError()
    }

    override fun getEntity(id: EntityId): Entity {
        val oId = requireOEntityId(id)
        if (oId == ORIDEntityId.EMPTY_ID) {
            throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        }
        val txn = currentOTransaction.activeSession.requireActiveTransaction()
        val vertex = txn.database.load<OVertex>(oId.asOId()) ?: throw EntityRemovedInDatabaseException(oId.getTypeName(), id)
        return OVertexEntity(vertex, this)
    }

    override fun getEntityTypeId(entityType: String): Int {
        val oClass = ODatabaseSession.getActiveSession().metadata.schema.getClass(entityType)
        return oClass?.defaultClusterId ?: -1
    }

    override fun getEntityType(entityTypeId: Int): String {
        val oClass =
            ODatabaseSession.getActiveSession().metadata.schema.getClassByClusterId(entityTypeId)
        return oClass.name
    }

    override fun renameEntityType(oldEntityTypeName: String, newEntityTypeName: String) {
        val oldClass = computeInTransaction {
            val txn = it as OStoreTransaction
            txn.activeSession.metadata.schema.getClass(oldEntityTypeName)
                ?: throw IllegalArgumentException("Class $oldEntityTypeName not found")
        }
        databaseProvider.acquireSession().apply {
            activateOnCurrentThread()
            oldClass.setName(newEntityTypeName)
            close()
        }
    }

    override fun getUsableSpace(): Long {
        return File(location).usableSpace
    }

    override fun getConfig(): PersistentEntityStoreConfig = config

    override fun getAsyncProcessor() = dummyJobProcessor

    override fun getStatistics() = dummyStatistics

    override fun getAndCheckCurrentTransaction(): StoreTransaction {
        val transaction = getCurrentTransaction()
            ?: throw java.lang.IllegalStateException("EntityStore: current transaction is not set.")
        return transaction
    }

    override fun getCountsAsyncProcessor() = dummyJobProcessor

    private val currentOTransaction get() = currentTransaction.get() as OStoreTransaction

    fun getOEntityId(entityId: PersistentEntityId): ORIDEntityId {
        return schemaBuddy.getOEntityId(entityId)
    }
}

internal fun PersistentEntityStore.requireOEntityId(id: EntityId): ORIDEntityId {
    return when (id) {
        is ORIDEntityId -> id
        PersistentEntityId.EMPTY_ID -> ORIDEntityId.EMPTY_ID
        is PersistentEntityId -> {
            val oEntityStore = this as? OPersistentEntityStore ?: throw IllegalArgumentException("OPersistentEntityStore is required to get OEntityId, the provided type is ${this.javaClass.simpleName}")
            oEntityStore.getOEntityId(id)
        }
        else -> throw IllegalArgumentException("${id.javaClass.simpleName} is not supported")
    }
}
