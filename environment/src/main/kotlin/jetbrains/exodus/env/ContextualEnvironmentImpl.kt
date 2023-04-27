/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.env

import jetbrains.exodus.ExodusException
import jetbrains.exodus.log.Log
import jetbrains.exodus.tree.TreeMetaInfo
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class ContextualEnvironmentImpl internal constructor(log: Log, ec: EnvironmentConfig) :
    EnvironmentImpl(log, ec), ContextualEnvironment {
    private val threadTxns: MutableMap<Thread, Deque<TransactionBase>> = ConcurrentHashMap(4, 0.75f, 4)
    override fun getCurrentTransaction(): TransactionBase? {
        val thread = Thread.currentThread()
        val stack = threadTxns[thread]
        return stack?.peek()
    }

    override fun getAndCheckCurrentTransaction(): Transaction {
        return currentTransaction
            ?: throw IllegalStateException("No transaction started in current thread")
    }

    override fun getAllStoreNames(): List<String> {
        return getAllStoreNames(getAndCheckCurrentTransaction())
    }

    override fun openBitmap(name: String, config: StoreConfig): ContextualBitmapImpl {
        if (config == StoreConfig.WITH_DUPLICATES || config == StoreConfig.WITH_DUPLICATES_WITH_PREFIXING) {
            throw ExodusException("Bitmap can't be opened on the store with duplicates")
        }
        val store = openStore("$name#bitmap", config)
        return ContextualBitmapImpl(store)
    }

    override fun openStore(name: String, config: StoreConfig): ContextualStoreImpl {
        return super.computeInTransaction { txn: Transaction ->
            openStore(
                name,
                config,
                txn
            )
        }
    }

    override fun openStore(name: String, config: StoreConfig, creationRequired: Boolean): ContextualStoreImpl? {
        return super.computeInTransaction { txn: Transaction ->
            openStore(
                name,
                config,
                txn,
                creationRequired
            )
        }
    }

    override fun openStore(name: String, config: StoreConfig, transaction: Transaction): ContextualStoreImpl {
        return super.openStore(name, config, transaction) as ContextualStoreImpl
    }

    override fun openStore(
        name: String,
        config: StoreConfig,
        transaction: Transaction,
        creationRequired: Boolean
    ): ContextualStoreImpl? {
        return super.openStore(name, config, transaction, creationRequired) as ContextualStoreImpl?
    }

    override fun createStore(name: String, metaInfo: TreeMetaInfo): StoreImpl {
        return ContextualStoreImpl(this, name, metaInfo)
    }

    override fun beginTransaction(beginHook: Runnable?, exclusive: Boolean, cloneMeta: Boolean): TransactionBase {
        val result = super.beginTransaction(beginHook, exclusive, cloneMeta)
        setCurrentTransaction(result)
        return result
    }

    override fun beginReadonlyTransaction(beginHook: Runnable?): TransactionBase {
        val result = super.beginReadonlyTransaction(beginHook)
        setCurrentTransaction(result)
        return result
    }

    override fun beginGCTransaction(): ReadWriteTransaction {
        val result = super.beginGCTransaction()
        setCurrentTransaction(result)
        return result
    }

    override fun executeInTransaction(executable: TransactionalExecutable) {
        val current: Transaction? = currentTransaction
        if (current != null) {
            executable.execute(current)
        } else {
            super.executeInTransaction(executable)
        }
    }

    override fun executeInExclusiveTransaction(executable: TransactionalExecutable) {
        val current: Transaction? = currentTransaction
        if (current == null) {
            super.executeInExclusiveTransaction(executable)
        } else {
            if (!current.isExclusive) {
                throw ExodusException("Current transaction should be exclusive")
            }
            executable.execute(current)
        }
    }

    override fun <T> computeInTransaction(computable: TransactionalComputable<T>): T {
        val current: Transaction? = currentTransaction
        return if (current != null) computable.compute(current) else super.computeInTransaction(computable)
    }

    override fun <T> computeInExclusiveTransaction(computable: TransactionalComputable<T>): T {
        val current = currentTransaction ?: return super.computeInExclusiveTransaction(computable)
        if (!current.isExclusive) {
            throw ExodusException("Current transaction should be exclusive")
        }
        return computable.compute(current)
    }

    private fun setCurrentTransaction(result: TransactionBase) {
        val thread = result.creatingThread
        var stack = threadTxns[thread]
        if (stack == null) {
            stack = ArrayDeque(4)
            threadTxns[thread] = stack
        }
        stack.push(result)
    }

    override fun finishTransaction(txn: TransactionBase) {
        val thread = txn.creatingThread
        if (Thread.currentThread() != thread) {
            throw ExodusException("Can't finish transaction in a thread different from the one which it was created in")
        }
        finishTransactionUnsafe(txn)
    }

    fun finishTransactionUnsafe(txn: TransactionBase) {
        val thread = txn.creatingThread
        val stack = threadTxns[thread] ?: throw ExodusException("Transaction was already finished")
        if (txn !== stack.peek()) {
            throw ExodusException("Can't finish transaction: nested transaction is not finished")
        }
        stack.pop()
        if (stack.isEmpty()) {
            threadTxns.remove(thread)
        }
        super.finishTransaction(txn)
    }

    override fun createTemporaryEmptyStore(name: String): StoreImpl {
        return ContextualTemporaryEmptyStore(this, name)
    }
}
