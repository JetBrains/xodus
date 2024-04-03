package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDbInternalAccessor.accessInternal
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.management.Statistics
import java.io.File
import java.util.concurrent.ConcurrentHashMap

class OPersistentEntityStore(
    private val db: OrientDB,
    private val userName: String,
    private val password: String,
    private val databaseName: String
) : PersistentEntityStore {

    private val typesMap = ConcurrentHashMap<String, Int>()
    private val config = PersistentEntityStoreConfig()
    private val dummyJobProcessor = object : MultiThreadDelegatingJobProcessor("dummy", 1) {}
    private val dummyStatistics = object : Statistics<Enum<*>>(arrayOf()) {}
    private val env = OEnvironment(db, this)

    override fun close() {}

    override fun getName() = databaseName

    override fun getLocation(): String {
        return db.accessInternal.basePath
    }

    override fun beginTransaction(): StoreTransaction {
        val session = db.cachedPool(databaseName, userName, password).acquire()
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
            txn.activeSession().commit()
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
            txn.activeSession().commit()
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
        require(id is OEntityId) { "Only OEntityId is supported, but was ${id.javaClass.simpleName}" }
        val txn = currentOTransaction.oTransaction
        val vertex = txn.database.load<OVertex>(id.asOId())
        return OVertexEntity(vertex, this)
    }

    override fun getEntityTypeId(entityType: String): Int {
        return typesMap.computeIfAbsent(entityType) {
            val oClass = ODatabaseSession.getActiveSession().metadata.schema.getClass(entityType)
            oClass?.defaultClusterId ?: -1
        }
    }

    override fun getEntityType(entityTypeId: Int): String {
        //This implementation is wierd
        val type = (typesMap.entries.firstOrNull { it.value == entityTypeId })?.key
        if (type == null) {
            val oClass =
                ODatabaseSession.getActiveSession().metadata.schema.classes.firstOrNull { it.defaultClusterId == entityTypeId }!!
            typesMap[oClass.name] = entityTypeId
            return oClass.name
        } else return type
    }

    override fun renameEntityType(oldEntityTypeName: String, newEntityTypeName: String) {
        val oldClass = computeInTransaction {
            val txn = it as OStoreTransaction
            txn.activeSession().metadata.schema.getClass(oldEntityTypeName)
                ?: throw IllegalStateException("Class found by name $oldEntityTypeName")
        }
        oldClass.setName(newEntityTypeName)
        typesMap.remove(oldEntityTypeName)
        typesMap.remove(newEntityTypeName)
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
}
