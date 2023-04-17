package jetbrains.exodus.env.management

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.management.MBeanBase
import java.util.concurrent.locks.ReentrantLock

class BackupController(private val env: EnvironmentImpl) : MBeanBase(getObjectName(env)),
    BackupControllerMBean {

    private var backupTransaction: Transaction? = null
    private val backupTransactionLock = ReentrantLock()

    override fun prepareBackup(): Array<String> {
        backupTransactionLock.lock()
        try {
            if (backupTransaction != null) {
                throw IllegalStateException("Backup is already in progress")
            }

            backupTransaction = env.beginReadonlyTransaction()
            return env.prepareForBackup()
        } finally {
            backupTransactionLock.unlock()
        }
    }

    override fun finishBackup() {
        backupTransactionLock.lock()
        try {
            if (backupTransaction == null) {
                throw IllegalStateException("Backup is not in progress")
            }

            backupTransaction!!.abort()
            backupTransaction = null
            env.finishBackup()
        } finally {
            backupTransactionLock.unlock()
        }
    }

    override fun close() {
        backupTransactionLock.lock()
        try {
            if (backupTransaction != null) {
                backupTransaction!!.abort()
                backupTransaction = null
            }
        } finally {
            backupTransactionLock.unlock()
        }

        super.close()
    }

    companion object {
        internal fun getObjectName(env: Environment) =
            "$BACKUP_CONTROLLER_NAME_PREFIX, location=${escapeLocation(env.location)}"
    }
}