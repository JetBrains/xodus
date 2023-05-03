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
import jetbrains.exodus.ExodusException
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.log.*
import jetbrains.exodus.tree.*
import jetbrains.exodus.tree.LongIterator
import jetbrains.exodus.tree.btree.BTreeEmpty
import mu.KLogging

class MetaTreeImpl(tree: ITree?, root: Long) : MetaTree {
    val tree: ITree
    val root: Long

    init {
        this.tree = tree!!
        this.root = root
    }

    override fun treeAddress(): Long {
        return tree.rootAddress
    }

    override fun rootAddress(): Long {
        return root
    }

    fun addressIterator(): LongIterator {
        return tree.addressIterator()
    }

    fun getMetaInfo(storeName: String, env: EnvironmentImpl): TreeMetaInfo? {
        val value = tree[StringBinding.stringToEntry(storeName)] ?: return null
        return TreeMetaInfo.load(env, value)
    }

    fun getRootAddress(structureId: Int): Long {
        val value = tree[LongBinding.longToCompressedEntry(structureId.toLong())]
        return if (value == null) Loggable.NULL_ADDRESS else CompressedUnsignedLongByteIterable.getLong(value)
    }

    val allStoreCount: Long
        get() {
            val size = tree.size
            if (size % 2L != 0L) {
                logger.error("MetaTree size is not even")
            }
            return size / 2
        }
    val allStoreNames: List<String>
        get() {
            val tree = tree
            if (tree.size == 0L) {
                return emptyList()
            }
            val result: MutableList<String> = ArrayList()
            tree.openCursor().use { cursor ->
                while (cursor.next) {
                    val key = ArrayByteIterable(cursor.key)
                    if (isStringKey(key)) {
                        val storeName = StringBinding.entryToString(key)
                        if (!EnvironmentImpl.isUtilizationProfile(storeName)) {
                            result.add(storeName)
                        }
                    }
                }
            }
            return result
        }

    fun getStoreNameByStructureId(structureId: Int, env: EnvironmentImpl): String? {
        tree.openCursor().use { cursor ->
            while (cursor.next) {
                val key = cursor.key
                if (isStringKey(ArrayByteIterable(key))) {
                    if (TreeMetaInfo.load(env, cursor.value).structureId == structureId) {
                        return StringBinding.entryToString(key)
                    }
                }
            }
        }
        return null
    }

    val clone: MetaTreeImpl
        get() = MetaTreeImpl(cloneTree(tree), root)

    class Proto(val address: Long, val root: Long) : MetaTreePrototype {
        override fun treeAddress(): Long {
            return address
        }

        override fun rootAddress(): Long {
            return root
        }
    }

    companion object : KLogging() {
        private const val EMPTY_LOG_BOUND = 5
        fun create(
            env: EnvironmentImpl,
            expired: ExpiredLoggableCollection
        ): Pair<MetaTreeImpl, Int> {
            val log = env.log
            val highAddress = log.highAddress
            if (highAddress > EMPTY_LOG_BOUND) {
                val rootLoggable: Loggable?
                val rootAddress = log.getStartUpDbRoot()
                rootLoggable = if (rootAddress >= 0) {
                    log.read(rootAddress)
                } else {
                    null
                }
                if (rootLoggable != null) {
                    val root = rootLoggable.address
                    var dbRoot: DatabaseRoot? = null
                    try {
                        dbRoot = DatabaseRoot(rootLoggable)
                    } catch (e: ExodusException) {
                        EnvironmentImpl.loggerError(
                            "Failed to load database root at " + rootLoggable.address,
                            e
                        )
                    }
                    if (dbRoot != null && dbRoot.isValid) {
                        try {
                            val metaTree = env.loadMetaTree(dbRoot.rootAddress)
                            return Pair(
                                MetaTreeImpl(metaTree, root),
                                dbRoot.lastStructureId
                            )
                        } catch (e: ExodusException) {
                            EnvironmentImpl.loggerError(
                                "Failed to recover to valid root" +
                                        LogUtil.getWrongAddressErrorMessage(
                                            dbRoot.address,
                                            env.environmentConfig.logFileSize shl 10
                                        ), e
                            )
                        }
                    }
                }
                log.use {
                    DataCorruptionException.raise("No valid root has found in the database", it, rootAddress)
                }
            }
            // no roots found: the database is empty
            logger.debug("No roots found: the database is empty")
            val resultTree = getEmptyMetaTree(env)
            val root: Long
            log.beginWrite()
            try {
                val rootAddress = resultTree.mutableCopy.save()
                root = log.write(
                    DatabaseRoot.DATABASE_ROOT_TYPE, Loggable.NO_STRUCTURE_ID,
                    DatabaseRoot.asByteIterable(rootAddress, EnvironmentImpl.META_TREE_ID), expired
                )
                log.flush()
                log.endWrite()
            } catch (t: Throwable) {
                throw ExodusException("Can't init meta tree in log", t)
            }
            return Pair<MetaTreeImpl, Int>(
                MetaTreeImpl(resultTree, root),
                EnvironmentImpl.META_TREE_ID
            )
        }

        fun create(
            env: EnvironmentImpl,
            prototype: MetaTreePrototype
        ): MetaTreeImpl {
            return MetaTreeImpl(
                env.loadMetaTree(prototype.treeAddress()),
                prototype.rootAddress()
            )
        }

        fun removeStore(out: ITreeMutable, storeName: String, id: Long) {
            out.delete(StringBinding.stringToEntry(storeName))
            out.delete(LongBinding.longToCompressedEntry(id))
        }

        fun addStore(out: ITreeMutable, storeName: String, metaInfo: TreeMetaInfo) {
            out.put(StringBinding.stringToEntry(storeName), metaInfo.toByteIterable())
        }

        fun saveTree(
            out: ITreeMutable,
            treeMutable: ITreeMutable
        ) {
            val treeRootAddress = treeMutable.save()
            val structureId = treeMutable.structureId
            out.put(
                LongBinding.longToCompressedEntry(structureId.toLong()),
                CompressedUnsignedLongByteIterable.getIterable(treeRootAddress)
            )
        }

        /**
         * Saves meta tree, writes database root and flushes the log.
         *
         * @param metaTree mutable meta tree
         * @param env      enclosing environment
         * @param expired  expired loggables (database root to be added)
         * @return database root loggable which is read again from the log.
         */
        fun saveMetaTree(
            metaTree: ITreeMutable,
            env: EnvironmentImpl,
            expired: ExpiredLoggableCollection
        ): Proto {
            val newMetaTreeAddress = metaTree.save()
            val log = env.log
            val lastStructureId = env.lastStructureId
            val dbRootAddress = log.write(
                DatabaseRoot.DATABASE_ROOT_TYPE, Loggable.NO_STRUCTURE_ID,
                DatabaseRoot.asByteIterable(newMetaTreeAddress, lastStructureId), expired
            )
            expired.add(dbRootAddress, (log.writtenHighAddress - dbRootAddress).toInt())
            return Proto(newMetaTreeAddress, dbRootAddress)
        }

        fun isStringKey(key: ArrayByteIterable): Boolean {
            // last byte of string is zero
            return key.byteAt(key.length - 1).toInt() == 0
        }

        fun cloneTree(tree: ITree): ITreeMutable {
            tree.openCursor().use { cursor ->
                val result = tree.mutableCopy
                while (cursor.next) {
                    result.put(cursor.key, cursor.value)
                }
                return result
            }
        }

        private fun getEmptyMetaTree(env: EnvironmentImpl): ITree {
            return object : BTreeEmpty(env.log, env.bTreeBalancePolicy, false, EnvironmentImpl.META_TREE_ID) {
                override fun getDataIterator(address: Long): DataIterator {
                    return DataIterator(log, address)
                }
            }
        }
    }
}
