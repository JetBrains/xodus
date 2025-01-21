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
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.entitystore.*

class YTDBPersistentEntityStore(
    private val databaseProvider: YTDBDatabaseProvider,
    private val name: String,
    private val schemaBuddy: YTDBSchemaBuddy = YTDBSchemaBuddyImpl(databaseProvider)
) : PersistentEntityStore, YTDBEntityStore {

    private val currentTransaction = ThreadLocal<YTDBStoreTransaction>()

    override val databaseSession: DatabaseSession
        get() {
            val tx = currentTransaction.get() ?: throw IllegalStateException("There is no active database session")
            return tx.databaseSession
        }

    override fun close() {
        //or it should be closed independently
        currentTransaction.get()?.abort()
        currentTransaction.remove()
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
        var currentTx: YTDBStoreTransaction? = currentTransaction.get()
        check(currentTx == null) { "EntityStore has a transaction on the current thread. Finish it before starting a new one." }

        val session = databaseProvider.acquireSession()

        currentTx = YTDBStoreTransactionImpl(
            session,
            store = this,
            schemaBuddy,
            onFinished = ::onTransactionFinished,
            onDeactivated = ::onTransactionDeactivated,
            onActivated = ::onTransactionActivated,
            readOnly = readOnly
        )
        currentTransaction.set(currentTx)
        currentTx.begin()

        return currentTx
    }

    private fun onTransactionFinished(session: DatabaseSession, tx: YTDBStoreTransaction) {
        check(currentTransaction.get() == tx) { "The current transaction at EntityStore is different for one that just has finished. It must not happen." }
        check(!session.isClosed) { "The session should not be closed at this point." }
        currentTransaction.remove()
        session.close()
    }

    private fun onTransactionDeactivated(session: DatabaseSession, tx: YTDBStoreTransaction) {
        check(currentTransaction.get() == tx) { "Impossible to deactivate the transaction. The transaction on the current thread is different from one that wants to suspend. It must not ever happen." }
        check(!tx.isFinished) { "Cannot deactivate a finished transaction" }
        check(!session.isClosed) { "Cannot deactivate a closed session" }
        currentTransaction.remove()
        DatabaseRecordThreadLocal.instance().remove()
    }

    private fun onTransactionActivated(session: DatabaseSession, tx: YTDBStoreTransaction) {
        check(currentTransaction.get() == null) { "Impossible to activate the transaction. There is already an active transaction on the current thread." }
        check(!hasActiveSession()) { "There is an active session on the current thread" }
        check(!tx.isFinished) { "Cannot activate a finished transaction" }
        check(!session.isClosed) { "Cannot activate a closed session" }
        session.activateOnCurrentThread()
        currentTransaction.set(tx)
    }

    override fun getCurrentTransaction(): StoreTransaction? {
        return currentTransaction.get()
    }

    override fun getBackupStrategy(): BackupStrategy {
        return object : BackupStrategy() {}
    }

    override fun executeInTransaction(executable: StoreTransactionalExecutable) {
        computeInTransaction { tx ->
            executable.execute(tx)
        }
    }

    override fun executeInExclusiveTransaction(executable: StoreTransactionalExecutable) =
        executeInTransaction(executable)

    override fun executeInReadonlyTransaction(executable: StoreTransactionalExecutable) =
        executeInTransaction(executable)

    override fun <T : Any?> computeInTransaction(computable: StoreTransactionalComputable<T>): T {
        val tx = beginTransaction() as YTDBStoreTransactionImpl
        try {
            val result = computable.compute(tx)
            tx.commit()
            return result
        } finally {
            if (!tx.isFinished) {
                tx.abort()
            }
        }
    }

    override fun <T : Any?> computeInExclusiveTransaction(computable: StoreTransactionalComputable<T>) =
        computeInTransaction(computable)

    override fun <T : Any?> computeInReadonlyTransaction(computable: StoreTransactionalComputable<T>) =
        computeInTransaction(computable)

    override fun registerCustomPropertyType(
        txn: StoreTransaction,
        clazz: Class<out Comparable<Any?>>,
        binding: ComparableBinding
    ) {
        throw NotImplementedError()
    }

    override fun getEntity(id: EntityId): Entity {
        val currentTx = requireActiveTransaction()
        return currentTx.getEntity(id)
    }

    override fun getEntityTypeId(entityType: String): Int {
        val currentTx = requireActiveTransaction()
        return currentTx.getTypeId(entityType)
    }

    override fun getEntityType(entityTypeId: Int): String {
        val currentTx = requireActiveTransaction()
       return currentTx.getType(entityTypeId)
    }

    override fun renameEntityType(oldEntityTypeName: String, newEntityTypeName: String) {
        val currentTx = requireActiveTransaction()
        currentTx.renameOClass(oldEntityTypeName, newEntityTypeName)
    }


    override fun getAndCheckCurrentTransaction(): StoreTransaction {
        return requireActiveTransaction()
    }

    override fun requireActiveTransaction(): YTDBStoreTransaction {
        val tx = currentTransaction.get()
        check(tx != null) { "No active transactions on the current thread" }
        tx.requireActiveTransaction()
        return tx
    }

    override fun requireActiveWritableTransaction(): YTDBStoreTransaction {
        val tx = currentTransaction.get()
        check(tx != null) { "No active transactions on the current thread" }
        tx.requireActiveWritableTransaction()
        return tx
    }

    override fun getOEntityId(entityId: PersistentEntityId): YTDBEntityId {
        return requireActiveTransaction().getOEntityId(entityId)
    }
}

internal fun YTDBEntityStore.requireOEntityId(id: EntityId): YTDBEntityId {
    return when (id) {
        is RIDEntityId -> id
        PersistentEntityId.EMPTY_ID -> RIDEntityId.EMPTY_ID
        is PersistentEntityId -> {
            val oEntityStore = this as? YTDBPersistentEntityStore ?: throw IllegalArgumentException("OPersistentEntityStore is required to get OEntityId, the provided type is ${this.javaClass.simpleName}")
            oEntityStore.getOEntityId(id)
        }
        else -> throw IllegalArgumentException("${id.javaClass.simpleName} is not supported")
    }
}
