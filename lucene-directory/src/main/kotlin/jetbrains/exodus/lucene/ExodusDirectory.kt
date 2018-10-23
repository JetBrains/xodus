/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.lucene

import jetbrains.exodus.env.ContextualEnvironment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.vfs.ClusteringStrategy
import jetbrains.exodus.vfs.FileNotFoundException
import jetbrains.exodus.vfs.VfsConfig
import jetbrains.exodus.vfs.VirtualFileSystem
import org.apache.lucene.index.IndexFileNames
import org.apache.lucene.store.*
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicLong

class ExodusDirectory(val environment: ContextualEnvironment,
                      vfsConfig: VfsConfig,
                      contentsStoreConfig: StoreConfig) : Directory() {

    val vfs: VirtualFileSystem = VirtualFileSystem(environment, vfsConfig, contentsStoreConfig)
    private val ticks = AtomicLong(System.currentTimeMillis())

    @JvmOverloads constructor(env: ContextualEnvironment,
                              contentsStoreConfig: StoreConfig = StoreConfig.WITHOUT_DUPLICATES) :
            this(env, createDefaultVfsConfig(), contentsStoreConfig)

    override fun listAll(): Array<String> {
        val txn = environment.andCheckCurrentTransaction
        val allFiles = ArrayList<String>(vfs.getNumberOfFiles(txn).toInt())
        for (file in vfs.getFiles(txn)) {
            allFiles.add(file.path)
        }
        return allFiles.toTypedArray()
    }

    override fun deleteFile(name: String) {
        vfs.deleteFile(environment.andCheckCurrentTransaction, name)
    }

    override fun fileLength(name: String) = environment.andCheckCurrentTransaction.let { txn ->
        vfs.getFileLength(txn, openExistingFile(txn, name))
    }

    override fun createOutput(name: String, context: IOContext): IndexOutput = ExodusIndexOutput(this, name)

    override fun createTempOutput(prefix: String, suffix: String, context: IOContext) =
            createOutput(IndexFileNames.segmentFileName(prefix, suffix + '_'.toString() + nextTicks(), "tmp"), context)

    override fun sync(names: Collection<String>) = syncMetaData()

    override fun rename(source: String, dest: String): Unit = environment.andCheckCurrentTransaction.let { txn ->
        vfs.renameFile(txn, openExistingFile(txn, source), dest)
    }

    override fun syncMetaData() = (environment as EnvironmentImpl).flushAndSync()

    @Throws(IOException::class)
    override fun openInput(name: String, context: IOContext): IndexInput {
        try {
            return ExodusIndexInput(this, name)
        } catch (e: FileNotFoundException) {
            // if index doesn't exist Lucene awaits an IOException
            throw java.io.FileNotFoundException(name)
        }
    }

    override fun openChecksumInput(name: String, context: IOContext): ChecksumIndexInput =
            FastSkippingBufferedChecksumIndexInput(openInput(name, context))

    override fun obtainLock(name: String) = NoLockFactory.INSTANCE.obtainLock(this, name)

    override fun close() = vfs.shutdown()

    internal fun nextTicks() = ticks.getAndIncrement()

    internal fun openExistingFile(txn: Transaction, name: String) =
            vfs.openFile(txn, name, false) ?: throw FileNotFoundException(name)

    companion object {

        private const val FIRST_CLUSTER_SIZE = 65536
        private const val MAX_CLUSTER_SIZE = 65536 * 16

        private fun createDefaultVfsConfig() = VfsConfig().apply {
            val clusteringStrategy = ClusteringStrategy.QuadraticClusteringStrategy(FIRST_CLUSTER_SIZE)
            clusteringStrategy.maxClusterSize = MAX_CLUSTER_SIZE
            this.clusteringStrategy = clusteringStrategy
        }
    }
}
