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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.core.dataStructures.decorators.HashMapDecorator
import jetbrains.exodus.env.MetaTreeImpl.Proto
import jetbrains.exodus.log.*
import jetbrains.exodus.tree.ExpiredLoggableCollection
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.ITreeMutable
import jetbrains.exodus.tree.TreeMetaInfo
import java.util.*

open class ReadWriteTransaction : TransactionBase {
    private val mutableTrees: Int2ObjectOpenHashMap<ITreeMutable>
    private val removedStores: Long2ObjectOpenHashMap<Pair<String, ITree?>>
    private val createdStores: MutableMap<String, TreeMetaInfo>
    final override val beginHook: Runnable?
    private var commitHook: Runnable? = null
    var replayCount: Int
        private set
    var acquiredPermits = 0

    internal constructor(
        env: EnvironmentImpl,
        beginHook: Runnable?,
        isExclusive: Boolean,
        cloneMeta: Boolean
    ) : super(env, isExclusive) {
        mutableTrees = Int2ObjectOpenHashMap()
        removedStores = Long2ObjectOpenHashMap()
        createdStores = HashMapDecorator()
        this.beginHook = Runnable {
            val currentMetaTree = env.metaTree as MetaTreeImpl
            metaTree = (if (cloneMeta) currentMetaTree.clone else currentMetaTree)
            env.registerTransaction(this@ReadWriteTransaction)
            beginHook?.run()
        }
        replayCount = 0
        @Suppress("LeakingThis")
        setExclusive(isExclusive() or env.shouldTransactionBeExclusive(this))
        @Suppress("LeakingThis")
        env.holdNewestSnapshotBy(this)
    }

    internal constructor(origin: TransactionBase, beginHook: Runnable?) : super(origin.environment, false) {
        mutableTrees = Int2ObjectOpenHashMap()
        removedStores = Long2ObjectOpenHashMap()
        createdStores = HashMapDecorator()
        val env = environment
        this.beginHook = getWrappedBeginHook(beginHook)
        replayCount = 0
        metaTree = origin.metaTree
        @Suppress("LeakingThis")
        isExclusive = env.shouldTransactionBeExclusive(this)
        @Suppress("LeakingThis")
        env.acquireTransaction(this)
        @Suppress("LeakingThis")
        env.registerTransaction(this)
    }

    override fun isIdempotent(): Boolean {
        return mutableTrees.isEmpty() && removedStores.isEmpty() && createdStores.isEmpty()
    }

    override fun abort() {
        checkIsFinished()
        clearImmutableTrees()
        doRevert()
        environment.finishTransaction(this)
    }

    override fun commit(): Boolean {
        checkIsFinished()
        return environment.commitTransaction(this)
    }

    override fun flush(): Boolean {
        checkIsFinished()
        val env = environment
        val result = env.flushTransaction(this, false)
        if (result) {
            // if the transaction was upgraded to exclusive during re-playing
            // then it should be downgraded back after successful flush().
            if (!wasCreatedExclusive() && isExclusive && env.environmentConfig.envTxnDowngradeAfterFlush) {
                env.downgradeTransaction(this)
                isExclusive = false
            }
            setStarted(System.currentTimeMillis())
        } else {
            incReplayCount()
        }
        return result
    }

    override fun revert() {
        checkIsFinished()
        if (isReadonly) {
            throw ExodusException("Attempt to revert read-only transaction")
        }
        val oldRoot = _metaTree!!.root
        val wasExclusive = isExclusive
        val env = environment
        if (isIdempotent) {
            env.holdNewestSnapshotBy(this, false)
        } else {
            doRevert()
            if (wasExclusive || !env.shouldTransactionBeExclusive(this)) {
                env.holdNewestSnapshotBy(this, false)
            } else {
                env.releaseTransaction(this)
                isExclusive = true
                env.holdNewestSnapshotBy(this)
            }
        }
        if (!env.isRegistered(this)) {
            throw ExodusException("Transaction should remain registered after revert")
        }
        if (invalidVersion(oldRoot)) {
            clearImmutableTrees()
            env.runTransactionSafeTasks()
        }
        setStarted(System.currentTimeMillis())
    }

    override fun setCommitHook(hook: Runnable?) {
        commitHook = hook
    }

    override fun isReadonly(): Boolean {
        return false
    }

    fun forceFlush(): Boolean {
        checkIsFinished()
        return environment.flushTransaction(this, true)
    }

    fun openStoreByStructureId(structureId: Int): StoreImpl {
        checkIsFinished()
        val env = environment
        val storeName = _metaTree!!.getStoreNameByStructureId(structureId, env)
        return if (storeName == null) TemporaryEmptyStore(env) else env.openStoreImpl(
            storeName,
            StoreConfig.USE_EXISTING,
            this,
            getTreeMetaInfo(storeName)
        )
    }

    override fun getTree(store: StoreImpl): ITree {
        checkIsFinished()
        return mutableTrees[store.structureId] ?: return super.getTree(store)
    }

    override fun getTreeMetaInfo(name: String): TreeMetaInfo? {
        checkIsFinished()
        val result = createdStores[name]
        return result ?: super.getTreeMetaInfo(name)
    }

    override fun storeRemoved(store: StoreImpl) {
        checkIsFinished()
        super.storeRemoved(store)
        val structureId = store.structureId
        val tree = store.openImmutableTree(_metaTree!!)
        removedStores.put(structureId.toLong(), Pair(store.name, tree))
        mutableTrees.remove(structureId)
    }

    fun storeOpened(store: StoreImpl) {
        removedStores.remove(store.structureId.toLong())
    }

    fun storeCreated(store: StoreImpl) {
        getMutableTree(store)
        createdStores[store.name] = store.metaInfo
    }

    private fun incReplayCount() {
        ++replayCount
    }

    fun isStoreNew(name: String): Boolean {
        return createdStores.containsKey(name)
    }

    fun doCommit(out: Array<Proto?>, log: Log): ExpiredLoggableCollection {
        val removedEntries = removedStores.long2ObjectEntrySet()
        var expiredLoggables = ExpiredLoggableCollection.newInstance(log)
        val metaTreeMutable = _metaTree!!.tree.mutableCopy
        for ((key, value) in removedEntries) {
            MetaTreeImpl.removeStore(metaTreeMutable, value.getFirst(), key)
            expiredLoggables = expiredLoggables.mergeWith(TreeMetaInfo.getTreeLoggables(value.getSecond()!!).trimToSize())
        }
        removedStores.clear()
        for ((key, value) in createdStores) {
            MetaTreeImpl.addStore(metaTreeMutable, key, value)
        }
        createdStores.clear()
        for (treeMutable in mutableTrees.values) {
            expiredLoggables = expiredLoggables.mergeWith(treeMutable.expiredLoggables.trimToSize())
            MetaTreeImpl.saveTree(metaTreeMutable, treeMutable)
        }
        clearImmutableTrees()
        mutableTrees.clear()
        expiredLoggables = expiredLoggables.mergeWith(metaTreeMutable.expiredLoggables.trimToSize())
        out[0] = MetaTreeImpl.saveMetaTree(metaTreeMutable, environment, expiredLoggables)
        return expiredLoggables
    }

    fun executeCommitHook() {
        if (commitHook != null) {
            commitHook!!.run()
        }
    }

    fun getMutableTree(store: StoreImpl): ITreeMutable {
        checkIsFinished()
        if (environment.environmentConfig.envTxnSingleThreadWrites) {
            val creatingThread = creatingThread
            if (creatingThread != Thread.currentThread()) {
                throw ExodusException("Can't create mutable tree in a thread different from the one which transaction was created in")
            }
        }
        val structureId = store.structureId
        var result = mutableTrees[structureId]
        if (result == null) {
            result = getTree(store).mutableCopy
            mutableTrees.put(structureId, result)
        }
        return result
    }

    /**
     * @param store opened store.
     * @return whether a mutable tree is created for specified store.
     */
    fun hasTreeMutable(store: StoreImpl): Boolean {
        return mutableTrees.containsKey(store.structureId)
    }

    fun removeTreeMutable(store: StoreImpl) {
        mutableTrees.remove(store.structureId)
    }

    override val allStoreNames: List<String>
        get() {
            var result = super.allStoreNames.toMutableList()
            if (createdStores.isEmpty()) return result
            if (result.isEmpty()) {
                result = ArrayList()
            }
            result.addAll(createdStores.keys)
            result.sort()
            return result
        }

    override fun setIsFinished(): Boolean {
        if (super.setIsFinished()) {
            mutableTrees.clear()
            return true
        }
        return false
    }

    private fun doRevert() {
        mutableTrees.clear()
        removedStores.clear()
        createdStores.clear()
    }
}
