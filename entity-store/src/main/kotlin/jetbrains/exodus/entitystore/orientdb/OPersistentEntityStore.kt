package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.management.Statistics
import java.io.File

class OPersistentEntityStore(
    val databaseProvider: ODatabaseProvider,
    private val name: String,
    private val classIdToOClassId: Map<Int, Int>
) : PersistentEntityStore {

    private val config = PersistentEntityStoreConfig()
    private val dummyJobProcessor = object : MultiThreadDelegatingJobProcessor("dummy", 1) {}
    private val dummyStatistics = object : Statistics<Enum<*>>(arrayOf()) {}
    private val env = OEnvironment(databaseProvider.database, this)


    override fun close() {
        //or it should be closed independently
        databaseProvider.close()
    }

    override fun getName() = name

    override fun getLocation(): String {
        return databaseProvider.databaseLocation
    }

    override fun beginTransaction(): StoreTransaction {
        val session = databaseProvider.acquireSession()
        session.activateOnCurrentThread()
        val txn = session.begin().transaction
        return OStoreTransactionImpl(session, txn, this)
    }

    override fun beginExclusiveTransaction(): StoreTransaction {
        return beginTransaction()
    }

    override fun beginReadonlyTransaction(): StoreTransaction {
        return beginTransaction()
    }

    override fun getCurrentTransaction(): StoreTransaction {
        return OStoreTransactionImpl(
            ODatabaseSession.getActiveSession(),
            ODatabaseSession.getActiveSession().transaction,
            this
        )
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
        } finally {
            // if txn has not already been aborted in execute()
            txn.activeSession.commit()
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
            return computable.compute(txn)
        } finally {
            // if txn has not already been aborted in execute()
            txn.activeSession.commit()
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
        val txn = currentOTransaction.oTransaction
        val vertex = txn.database.load<OVertex>(oId.asOId())
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
                ?: throw IllegalStateException("Class found by name $oldEntityTypeName")
        }
        oldClass.setName(newEntityTypeName)
    }

    override fun getUsableSpace(): Long {
        return File(location).usableSpace
    }

    override fun getConfig(): PersistentEntityStoreConfig = config

    override fun getAsyncProcessor() = dummyJobProcessor

    override fun getStatistics() = dummyStatistics

    override fun getAndCheckCurrentTransaction() = currentTransaction

    override fun getCountsAsyncProcessor() = dummyJobProcessor


    private val currentOTransaction get() = currentTransaction as OStoreTransaction

    fun getOEntityId(entityId: PersistentEntityId): OEntityId {
        // Keep in mind that it is possible that we are given an entityId that is not in the database.
        // It is a valid case.

        val oSession = ODatabaseSession.getActiveSession() ?: throw IllegalStateException("no active database session found")
        val classId = entityId.typeId
        val localEntityId = entityId.localId
        val oClassId = classIdToOClassId[classId] ?: return ORIDEntityId.EMPTY_ID
        val className = oSession.getClusterNameById(oClassId) ?: return ORIDEntityId.EMPTY_ID
        val oClass = oSession.getClass(className) ?: return ORIDEntityId.EMPTY_ID

        val resultSet: OResultSet = oSession.query("SELECT FROM $className WHERE ${OVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME} = ?", localEntityId)
        val oid = if (resultSet.hasNext()) {
            val result = resultSet.next()
            result.toVertex()?.identity ?: return ORIDEntityId.EMPTY_ID
        } else {
            return ORIDEntityId.EMPTY_ID
        }

        return ORIDEntityId(classId, localEntityId, oid, oClass)
    }
}

internal fun PersistentEntityStore.requireOEntityId(id: EntityId): OEntityId {
    return when (id) {
        is OEntityId -> id
        PersistentEntityId.EMPTY_ID -> ORIDEntityId.EMPTY_ID
        is PersistentEntityId -> {
            val oEntityStore = this as? OPersistentEntityStore ?: throw IllegalArgumentException("OPersistentEntityStore is required to get OEntityId, the provided type is ${this.javaClass.simpleName}")
            oEntityStore.getOEntityId(id)
        }
        else -> throw IllegalArgumentException("${id.javaClass.simpleName} is not supported")
    }
}

fun ODatabaseSession.getClassIdToOClassIdMap(): Map<Int, Int> = buildMap {
    for (oClass in metadata.schema.classes) {
        if (oClass.isVertexType && oClass.name != OClass.VERTEX_CLASS_NAME) {
            put(oClass.requireClassId(), oClass.defaultClusterId)
        }
    }
}
