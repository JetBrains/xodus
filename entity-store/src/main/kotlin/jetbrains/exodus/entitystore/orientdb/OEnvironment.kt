package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.OrientDB
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.env.*
import jetbrains.exodus.management.Statistics

open class OEnvironment(
    private val db: OrientDB,
    private val store: OPersistentEntityStore
) : Environment {
    private val created = System.currentTimeMillis()
    private val config = EnvironmentConfig()
    private val statistics = object : Statistics<Enum<*>>(arrayOf()) {}
    override fun close() {
        db.close()
    }

    override fun getBackupStrategy(): BackupStrategy {
        throw NotImplementedError()
    }

    override fun getCreated(): Long {
        return created
    }

    override fun getLocation(): String {
        return store.location
    }

    override fun openBitmap(name: String, config: StoreConfig, transaction: Transaction): Bitmap {
        throw NotImplementedError()
    }

    override fun openStore(name: String, config: StoreConfig, transaction: Transaction): Store {
        throw NotImplementedError()
    }

    override fun openStore(
        name: String,
        config: StoreConfig,
        transaction: Transaction,
        creationRequired: Boolean
    ): Store? {
        throw NotImplementedError()
    }

    override fun executeTransactionSafeTask(task: Runnable) {
        store.computeInTransaction {
            task.run()
        }
    }

    override fun clear() {
       store.clear()
    }

    override fun isReadOnly(): Boolean {
        return false
    }

    override fun isOpen(): Boolean {
        return db.isOpen
    }

    override fun getAllStoreNames(txn: Transaction): MutableList<String> {
        throw NotImplementedError()
    }

    override fun storeExists(storeName: String, txn: Transaction): Boolean {
        throw NotImplementedError()
    }

    override fun truncateStore(storeName: String, txn: Transaction) {
        throw NotImplementedError()
    }

    override fun removeStore(storeName: String, txn: Transaction) {
        throw NotImplementedError()
    }

    override fun gc() {
        throw NotImplementedError()
    }

    override fun suspendGC() {
        throw NotImplementedError()
    }

    override fun resumeGC() {
        throw NotImplementedError()
    }

    override fun beginTransaction(): Transaction {
        val txn = store.beginTransaction() as OStoreTransaction
        return OEnvironmentTransaction(this, txn)
    }

    override fun beginTransaction(beginHook: Runnable?): Transaction {
        return beginTransaction()
    }

    override fun beginExclusiveTransaction(): Transaction {
        return beginTransaction()
    }

    override fun beginExclusiveTransaction(beginHook: Runnable?): Transaction {
        return beginTransaction()
    }

    override fun beginReadonlyTransaction(): Transaction {
        return beginTransaction()
    }

    override fun beginReadonlyTransaction(beginHook: Runnable?): Transaction {
        return beginTransaction()
    }

    override fun executeInTransaction(executable: TransactionalExecutable) {
        val txn = beginTransaction()
        executable.execute(txn)
        txn.commit()
    }

    override fun executeInExclusiveTransaction(executable: TransactionalExecutable) {
        executeInTransaction(executable)
    }

    override fun executeInReadonlyTransaction(executable: TransactionalExecutable) {
        executeInTransaction(executable)
    }

    override fun <T : Any?> computeInTransaction(computable: TransactionalComputable<T>): T {
        val txn = beginTransaction()
        return try {
            computable.compute(txn)
        } finally {
            txn.commit()
        }
    }

    override fun <T : Any?> computeInExclusiveTransaction(computable: TransactionalComputable<T>): T {
        return computeInTransaction(computable)
    }

    override fun <T : Any?> computeInReadonlyTransaction(computable: TransactionalComputable<T>): T {
        return computeInTransaction(computable)
    }

    override fun getEnvironmentConfig(): EnvironmentConfig {
        return config
    }

    override fun getStatistics(): Statistics<out Enum<*>> {
        return statistics
    }

    override fun getCipherProvider(): StreamCipherProvider? {
        throw NotImplementedError()
    }

    override fun getCipherKey(): ByteArray? {
        throw NotImplementedError()
    }

    override fun getCipherBasicIV(): Long {
        throw NotImplementedError()
    }

    override fun executeBeforeGc(action: Runnable?) {
        throw NotImplementedError()
    }
}
