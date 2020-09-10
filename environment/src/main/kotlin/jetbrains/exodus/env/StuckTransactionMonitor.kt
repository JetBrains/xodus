/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.env

import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.ThreadJobProcessorPool
import mu.KLogging
import java.lang.ref.WeakReference
import java.util.*

internal class StuckTransactionMonitor(env: EnvironmentImpl) : Job() {

    private val envRef: WeakReference<EnvironmentImpl> = WeakReference(env)

    init {
        processor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared stuck transaction monitor")
        queueThis(env)
    }

    var stuckTxnCount: Int = 0
        private set

    private val env: EnvironmentImpl? get() = envRef.get()

    override fun execute() {
        val env = this.env
        if (env != null && env.isOpen) {
            var stuckTxnCount = 0
            try {
                env.transactionTimeout().forEachExpiredTransaction { txn ->
                    txn.trace?.let { trace ->
                        val creatingThread = txn.creatingThread
                        val msg = "Transaction timed out: created at ${Date(txn.startTime)}, " +
                                "thread = $creatingThread(${creatingThread.id})\n$trace"
                        logger.info(msg)
                        ++stuckTxnCount
                    }
                }
                env.transactionExpirationTimeout().forEachExpiredTransaction { txn ->
                    if (env is ContextualEnvironmentImpl) {
                        env.finishTransactionUnsafe(txn)
                    } else {
                        env.finishTransaction(txn)
                    }
                }
            } finally {
                this.stuckTxnCount = stuckTxnCount
                queueThis(env)
            }
        }
    }

    private fun queueThis(env: EnvironmentImpl) {
        processor.queueIn(this, env.environmentConfig.envMonitorTxnsCheckFreq.toLong())
    }

    private fun Int.forEachExpiredTransaction(callback: (TransactionBase) -> Unit) {
        if (this != 0) {
            val timeBound = System.currentTimeMillis() - this
            env?.forEachActiveTransaction {
                val txn = it as TransactionBase
                if (txn.startTime < timeBound) {
                    callback(it)
                }
            }
        }
    }

    companion object : KLogging()
}
