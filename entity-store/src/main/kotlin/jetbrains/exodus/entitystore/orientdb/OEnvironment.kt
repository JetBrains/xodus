package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.OrientDB
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.env.*
import jetbrains.exodus.management.Statistics

open class OEnvironment(
    private val db: OrientDB, private val name: String,
    private val store: OPersistentStore
) : Environment {
    private val created = System.currentTimeMillis()
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
        TODO("Not yet implemented")
    }

    override fun openStore(name: String, config: StoreConfig, transaction: Transaction): Store {
        TODO("Not yet implemented")
    }

    override fun openStore(
        name: String,
        config: StoreConfig,
        transaction: Transaction,
        creationRequired: Boolean
    ): Store? {
        TODO("Not yet implemented")
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
        return arrayListOf()
    }

    override fun storeExists(storeName: String, txn: Transaction): Boolean {
        return false
    }

    override fun truncateStore(storeName: String, txn: Transaction) {

    }

    override fun removeStore(storeName: String, txn: Transaction) {

    }

    override fun gc() {

    }

    override fun suspendGC() {

    }

    override fun resumeGC() {

    }

    override fun beginTransaction(): Transaction {
        TODO("Not yet implemented")
    }

    override fun beginTransaction(beginHook: Runnable?): Transaction {
        TODO("Not yet implemented")
    }

    override fun beginExclusiveTransaction(): Transaction {
        TODO("Not yet implemented")
    }

    override fun beginExclusiveTransaction(beginHook: Runnable?): Transaction {
        TODO("Not yet implemented")
    }

    override fun beginReadonlyTransaction(): Transaction {
        TODO("Not yet implemented")
    }

    override fun beginReadonlyTransaction(beginHook: Runnable?): Transaction {
        TODO("Not yet implemented")
    }

    override fun executeInTransaction(executable: TransactionalExecutable) {
        TODO("Not yet implemented")
    }

    override fun executeInExclusiveTransaction(executable: TransactionalExecutable) {
        TODO("Not yet implemented")
    }

    override fun executeInReadonlyTransaction(executable: TransactionalExecutable) {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> computeInTransaction(computable: TransactionalComputable<T>): T {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> computeInExclusiveTransaction(computable: TransactionalComputable<T>): T {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> computeInReadonlyTransaction(computable: TransactionalComputable<T>): T {
        TODO("Not yet implemented")
    }

    override fun getEnvironmentConfig(): EnvironmentConfig {
        TODO("Not yet implemented")
    }

    override fun getStatistics(): Statistics<out Enum<*>> {
        TODO("Not yet implemented")
    }

    override fun getCipherProvider(): StreamCipherProvider? {
        TODO("Not yet implemented")
    }

    override fun getCipherKey(): ByteArray {
        TODO("Not yet implemented")
    }

    override fun getCipherBasicIV(): Long {
        TODO("Not yet implemented")
    }

    override fun executeBeforeGc(action: Runnable?) {
        TODO("Not yet implemented")
    }
}
