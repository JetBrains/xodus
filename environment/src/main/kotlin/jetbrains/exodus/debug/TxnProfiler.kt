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
package jetbrains.exodus.debug

import jetbrains.exodus.env.TransactionBase
import mu.KLogging
import java.io.PrintStream

class TxnProfiler : KLogging() {

    var gcTransactions: Long = 0L
        private set
    var gcMovedBytes: Long = 0L
        private set
    private val readonlyTxns = StackTraceMap()
    private val txnCounts = StackTraceMap()
    private val txnWrittenBytes = StackTraceMap()

    fun incGcTransaction() = ++gcTransactions

    fun addGcMovedBytes(bytes: Long) {
        gcMovedBytes += bytes
    }

    fun addReadonlyTxn(txn: TransactionBase) {
        readonlyTxns.add(checkNotNull(txn.trace))
    }

    fun addTxn(txn: TransactionBase, writtenBytes: Long) {
        checkNotNull(txn.trace).let { trace ->
            txnCounts.add(trace)
            txnWrittenBytes.add(trace, writtenBytes)
        }
    }

    fun reset() {
        gcTransactions = 0L
        gcMovedBytes = 0L
        readonlyTxns.clear()
        txnCounts.clear()
        txnWrittenBytes.clear()
    }

    fun dump() = logger.info { dumpToString { ps -> dump(ps) } }

    private fun dump(ps: PrintStream) {
        ps.println("GC transaction: $gcTransactions")
        ps.println("Bytes moved by GC: $gcMovedBytes")
        ps.println("Read-only transactions:")
        readonlyTxns.forEach { st, count ->
            ps.println(count)
            st.toString(ps)
        }
        ps.println("Transaction counts:")
        txnCounts.forEach { st, count ->
            ps.println(count)
            st.toString(ps)
        }
        ps.println("Transaction traffic (written bytes):")
        txnWrittenBytes.forEach { st, bytes ->
            ps.print(bytes)
            ps.println(" bytes")
            st.toString(ps)
        }
    }
}