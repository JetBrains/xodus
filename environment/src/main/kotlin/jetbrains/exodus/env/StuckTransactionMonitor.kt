package jetbrains.exodus.env

import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.ThreadJobProcessorPool
import mu.KLogging
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.util.*

internal class StuckTransactionMonitor(private val env: EnvironmentImpl) : Job() {

    init {
        processor = ThreadJobProcessorPool.getOrCreateJobProcessor("Exodus shared stuck transaction monitor")
        queueThis()
    }

    var errorMessage: String? = null
        private set

    override fun execute() {
        if (env.isOpen) {
            val prevErrorMessage = errorMessage
            try {
                val transactionTimeout = env.transactionTimeout()
                if (transactionTimeout != 0) {
                    val creationTimeBound = System.currentTimeMillis() - transactionTimeout
                    env.forEachActiveTransaction {
                        val txn = it as TransactionBase
                        val trace = txn.trace
                        val created = txn.startTime
                        if (trace != null && created < creationTimeBound) {
                            val creatingThread = txn.creatingThread
                            val out = ByteArrayOutputStream()
                            val ps = PrintStream(out)
                            val errorHeader = "Transaction timed out: created at ${Date(created)}, thread = $creatingThread(${creatingThread.id})"
                            ps.writer().write(errorHeader)
                            trace.printStackTrace(ps)
                            logger.error(errorHeader, trace)
                            errorMessage = out.toString(Charsets.UTF_8.name())
                        }
                    }
                }
            } finally {
                // if error message didn't change then clear it
                if (prevErrorMessage === errorMessage) {
                    errorMessage = null
                }
                queueThis()
            }
        }
    }

    private fun queueThis() {
        processor.queueIn(this, env.environmentConfig.envMonitorTxnsCheckFreq.toLong())
    }

    companion object : KLogging()
}
