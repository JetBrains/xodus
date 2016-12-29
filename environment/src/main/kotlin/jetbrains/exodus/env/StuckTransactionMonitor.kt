package jetbrains.exodus.env

import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.ThreadJobProcessorPool
import mu.KLogging
import java.util.*

internal class StuckTransactionMonitor(private val env: EnvironmentImpl) : Job() {

    init {
        processor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared stuck transaction monitor")
        queueThis()
    }

    override fun execute() {
        if (env.isOpen) {
            try {
                val transactionTimeout = env.transactionTimeout()
                if (transactionTimeout != 0) {
                    val creationTimeBound = System.currentTimeMillis() - transactionTimeout
                    env.forEachActiveTransaction {
                        val txn = it as TransactionBase
                        val created = txn.startTime
                        if (created < creationTimeBound) {
                            val creatingThread = txn.creatingThread
                            logger.error("Transaction timed out: created at " +
                                    "${Date(created)}, thread = $creatingThread(${creatingThread.id})", txn.trace)
                        }
                    }
                }
            } finally {
                queueThis()
            }
        }
    }

    private fun queueThis() {
        processor.queueIn(this, env.environmentConfig.envMonitorTxnsCheckFreq.toLong())
    }

    companion object : KLogging()
}
