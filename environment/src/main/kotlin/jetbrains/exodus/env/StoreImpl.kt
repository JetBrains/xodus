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

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.ITreeMutable
import jetbrains.exodus.tree.TreeCursorMutable
import jetbrains.exodus.tree.TreeMetaInfo
import jetbrains.exodus.tree.btree.BTree
import jetbrains.exodus.tree.btree.BTreeEmpty
import jetbrains.exodus.tree.patricia.PatriciaTree
import jetbrains.exodus.tree.patricia.PatriciaTreeEmpty
import jetbrains.exodus.tree.patricia.PatriciaTreeWithDuplicates
import jetbrains.exodus.util.StringInterner.Companion.intern

open class StoreImpl internal constructor(
    private val environment: EnvironmentImpl,
    name: String,
    val metaInfo: TreeMetaInfo
) : Store {
    private val name: String

    init {
        this.name = intern(name)!!
    }

    override fun getEnvironment(): EnvironmentImpl {
        return environment
    }

    override fun get(txn: Transaction, key: ByteIterable): ByteIterable? {
        val tx = txn as TransactionBase
        val tree = tx.getTree(this)
        if (!tx.isDisableStoreGetCache) {
            val storeGetCache = environment.storeGetCache
            if (storeGetCache != null) {
                val treeRootAddress = tree.getRootAddress()
                val useStoreGetCache =
                    treeRootAddress != Loggable.NULL_ADDRESS && tree.size() >= storeGetCache.minTreeSize
                // if neither tree is empty nor mutable
                if (useStoreGetCache) {
                    var result = storeGetCache.tryKey(treeRootAddress, key)
                    if (result != null) {
                        return if (result === NULL_CACHED_VALUE) null else result
                    }
                    result = tree[key]
                    val cachedValue: ArrayByteIterable = when (result) {
                        null -> {
                            NULL_CACHED_VALUE
                        }

                        is ArrayByteIterable -> {
                            result
                        }

                        else -> {
                            ArrayByteIterable(result)
                        }
                    }
                    if (cachedValue.length <= storeGetCache.maxValueSize) {
                        storeGetCache.cacheObject(treeRootAddress, key, cachedValue)
                    }
                    return result
                }
            }
        }
        return tree[key]
    }

    override fun exists(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ): Boolean {
        return (txn as TransactionBase).getTree(this).hasPair(key, value)
    }

    override fun put(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ): Boolean {
        val mutableTree: ITreeMutable =
            EnvironmentImpl.throwIfReadonly(txn, "Can't put in read-only transaction").getMutableTree(this)
        if (mutableTree.put(key, value)) {
            TreeCursorMutable.notifyCursors(mutableTree)
            return true
        }
        return false
    }

    fun putNotifyNoCursors(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ): Boolean {
        return EnvironmentImpl.throwIfReadonly(txn, "Can't put in read-only transaction").getMutableTree(this)
            .put(key, value)
    }

    override fun putRight(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ) {
        val mutableTree: ITreeMutable =
            EnvironmentImpl.throwIfReadonly(txn, "Can't put in read-only transaction").getMutableTree(this)
        mutableTree.putRight(key, value)
        TreeCursorMutable.notifyCursors(mutableTree)
    }

    override fun add(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ): Boolean {
        val mutableTree: ITreeMutable =
            EnvironmentImpl.throwIfReadonly(txn, "Can't add in read-only transaction").getMutableTree(this)
        if (mutableTree.add(key, value)) {
            TreeCursorMutable.notifyCursors(mutableTree)
            return true
        }
        return false
    }

    override fun count(txn: Transaction): Long {
        return (txn as TransactionBase).getTree(this).size()
    }

    override fun openCursor(txn: Transaction): Cursor {
        return CursorImpl(this, txn as TransactionBase)
    }

    override fun delete(
        txn: Transaction,
        key: ByteIterable
    ): Boolean {
        val mutableTree: ITreeMutable =
            EnvironmentImpl.throwIfReadonly(txn, "Can't delete in read-only transaction").getMutableTree(this)
        if (mutableTree.delete(key)) {
            TreeCursorMutable.notifyCursors(mutableTree)
            return true
        }
        return false
    }

    override fun getName(): String {
        return name
    }

    @Deprecated("Deprecated in Java")
    override fun close() {
    }

    override fun getConfig(): StoreConfig {
        return TreeMetaInfo.toConfig(metaInfo)
    }

    fun isNew(txn: Transaction): Boolean {
        return !txn.isReadonly && (txn as ReadWriteTransaction).isStoreNew(name)
    }

    fun persistCreation(txn: Transaction) {
        EnvironmentImpl.throwIfReadonly(txn, "Read-only transaction is not enough").storeCreated(this)
    }

    open fun reclaim(
        transaction: Transaction,
        loggable: RandomAccessLoggable,
        loggables: Iterator<RandomAccessLoggable>
    ) {
        val txn: ReadWriteTransaction =
            EnvironmentImpl.throwIfReadonly(transaction, "Can't reclaim in read-only transaction")
        val hadTreeMutated = txn.hasTreeMutable(this)
        if (!txn.getMutableTree(this).reclaim(loggable, loggables) && !hadTreeMutated) {
            txn.removeTreeMutable(this)
        }
    }

    fun openImmutableTree(metaTree: MetaTreeImpl): ITree {
        val structureId = structureId
        val upToDateRootAddress = metaTree.getRootAddress(structureId)
        val hasDuplicates = metaInfo.hasDuplicates()
        val treeIsEmpty = upToDateRootAddress == Loggable.NULL_ADDRESS
        val log = environment.log
        val result: ITree = if (!metaInfo.isKeyPrefixing()) {
            val balancePolicy = environment.bTreeBalancePolicy
            if (treeIsEmpty) BTreeEmpty(log, balancePolicy, hasDuplicates, structureId) else BTree(
                log,
                balancePolicy,
                upToDateRootAddress,
                hasDuplicates,
                structureId
            )
        } else {
            if (treeIsEmpty) {
                PatriciaTreeEmpty(log, structureId, hasDuplicates)
            } else {
                if (hasDuplicates) PatriciaTreeWithDuplicates(log, upToDateRootAddress, structureId) else PatriciaTree(
                    log,
                    upToDateRootAddress,
                    structureId
                )
            }
        }
        return result
    }

    val structureId: Int
        get() = metaInfo.structureId

    companion object {
        private val NULL_CACHED_VALUE = ArrayByteIterable(ByteIterable.EMPTY)
    }
}