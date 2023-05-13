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

import jetbrains.exodus.core.dataStructures.decorators.HashMapDecorator
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.debug.StackTrace
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.TreeMetaInfo
import org.slf4j.LoggerFactory
import java.io.PrintWriter
import java.io.StringWriter

/**
 * Base class for transactions.
 */
abstract class TransactionBase(private val env: EnvironmentImpl, private var isExclusive: Boolean) : Transaction {
    @JvmField
    val creatingThread: Thread = Thread.currentThread()

    @JvmField
    protected var metaTree: MetaTreeImpl? = null

    internal fun getMetaTree(): MetaTree? = metaTree

    internal fun setMetaTree(metaTree: MetaTree?) {
        this.metaTree = metaTree as MetaTreeImpl
    }


    private val immutableTrees: IntHashMap<ITree> = IntHashMap()
    private val userObjects: MutableMap<Any, Any>

    @JvmField
    val trace: StackTrace?

    @JvmField
    val created // created is the ticks when the txn was actually created (constructed)
            : Long
    private var started // started is the ticks when the txn held its current snapshot
            : Long
    private val wasCreatedExclusive: Boolean = isExclusive
    private var traceFinish: Array<StackTraceElement>?

    @JvmField
    var isDisableStoreGetCache = false
    private var beforeTransactionFlushAction: Runnable? = null

    init {
        userObjects = HashMapDecorator()
        trace = if (env.transactionTimeout() > 0 || env.environmentConfig.profilerEnabled) StackTrace() else null
        created = System.currentTimeMillis()
        started = created
        traceFinish = null
    }

    override fun getSnapshot(): Transaction {
        return getSnapshot(null)
    }

    override fun getSnapshot(beginHook: Runnable?): Transaction {
        checkIsFinished()
        return ReadWriteTransaction(this, beginHook)
    }

    override fun getReadonlySnapshot(): Transaction {
        checkIsFinished()
        return ReadonlyTransaction(this)
    }

    override fun getEnvironment(): EnvironmentImpl {
        return env
    }

    override fun getStartTime(): Long {
        return started
    }

    override fun getSnapshotId(): Long {
        return metaTree!!.root
    }

    final override fun isExclusive(): Boolean {
        return isExclusive
    }

    override fun isFinished(): Boolean {
        return traceFinish != null
    }

    override fun getUserObject(key: Any): Any? {
        synchronized(userObjects) { return userObjects[key] }
    }

    override fun setUserObject(key: Any, value: Any) {
        synchronized(userObjects) { userObjects.put(key, value) }
    }

    open fun getTree(store: StoreImpl): ITree {
        checkIsFinished()
        val structureId = store.getStructureId()
        var result = immutableTrees[structureId]
        if (result == null) {
            result = store.openImmutableTree(metaTree!!)
            synchronized(immutableTrees) { immutableTrees.put(structureId, result) }
        }
        return result
    }

    fun checkIsFinished() {
        if (isFinished) {
            if (traceFinish != null && traceFinish!!.isNotEmpty()) {
                val stringWriter = StringWriter()
                val printWriter = PrintWriter(stringWriter)
                printWriter.println("Transaction is expected to be active but already finished at : ")
                LogUtil.printStackTrace(traceFinish!!, printWriter)
                printWriter.flush()
                val message = stringWriter.toString()
                logger.error(message)
            }
            throw TransactionFinishedException()
        }
    }

    fun getRoot(): Long = metaTree!!.root

    fun invalidVersion(root: Long): Boolean {
        return metaTree == null || metaTree!!.root != root
    }

    fun setStarted(started: Long) {
        this.started = started
    }

    fun wasCreatedExclusive(): Boolean {
        return wasCreatedExclusive
    }

    open fun isGCTransaction(): Boolean = false

    open fun getTreeMetaInfo(name: String): TreeMetaInfo? {
        checkIsFinished()
        return metaTree!!.getMetaInfo(name, env)
    }

    open fun storeRemoved(store: StoreImpl) {
        checkIsFinished()
        synchronized(immutableTrees) { immutableTrees.remove(store.getStructureId()) }
    }

    open fun getAllStoreNames(): List<String> {
        checkIsFinished()
        return metaTree!!.getAllStoreNames()
    }

    abstract fun beginHook(): Runnable?
    protected fun clearImmutableTrees() {
        synchronized(immutableTrees) { immutableTrees.clear() }
    }

    fun setBeforeTransactionFlushAction(exec: Runnable) {
        beforeTransactionFlushAction = exec
    }

    fun executeBeforeTransactionFlushAction() {
        if (beforeTransactionFlushAction != null) {
            beforeTransactionFlushAction!!.run()
        }
    }

    fun setExclusive(isExclusive: Boolean) {
        this.isExclusive = isExclusive
    }

    open fun setIsFinished(): Boolean {
        if (traceFinish == null) {
            clearImmutableTrees()
            synchronized(userObjects) { userObjects.clear() }
            traceFinish =
                if (env.environmentConfig.isEnvTxnTraceFinish) Thread.currentThread().stackTrace else emptyArray()
            return true
        }
        return false
    }

    protected fun getWrappedBeginHook(beginHook: Runnable?): Runnable {
        return Runnable {
            val env = environment
            metaTree = env.getMetaTree() as MetaTreeImpl
            env.registerTransaction(this@TransactionBase)
            beginHook?.run()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TransactionBase::class.java)
    }
}
