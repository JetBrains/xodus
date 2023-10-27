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

    override fun prepareBackup()  {
        backupTransactionLock.lock()
        try {
            if (backupTransaction != null) {
                throw IllegalStateException("Backup is already in progress")
            }

            backupTransaction = env.beginReadOnlyUnmonitoredTransaction()
            return env.prepareForBackup()
        } finally {
            backupTransactionLock.unlock()
        }
    }

    override fun finishBackup() {
        backupTransactionLock.lock()
        try {
            if (backupTransaction == null) {
                return
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