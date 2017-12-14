/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.vfs

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.kotlin.notNull
import java.io.Closeable

internal class ClusterIterator @JvmOverloads constructor(private val vfs: VirtualFileSystem,
                                                         txn: Transaction,
                                                         private val fd: Long,
                                                         position: Long = 0L) : Closeable {
    private val cursor: Cursor = vfs.contents.openCursor(txn)
    var current: Cluster? = null
        private set
    var isClosed: Boolean = false
        private set

    constructor(vfs: VirtualFileSystem,
                txn: Transaction,
                file: File) : this(vfs, txn, file.descriptor)

    init {
        seek(position)
        isClosed = false
    }

    /**
     * Seeks to the cluster that contains data by position. Doesn't navigate within cluster itself.
     *
     * @param position position in the file
     */
    fun seek(position: Long) {
        var pos = position
        val cs = vfs.config.clusteringStrategy
        val it: ByteIterable?
        if (cs.isLinear) {
            // if clustering strategy is linear then all clusters has the same size
            val firstClusterSize = cs.firstClusterSize
            it = cursor.getSearchKeyRange(ClusterKey.toByteIterable(fd, pos / firstClusterSize))
            if (it == null) {
                current = null
            } else {
                current = readCluster(it)
                adjustCurrentCluster()
                val currentCluster = this.current
                if (currentCluster != null) {
                    currentCluster.startingPosition = currentCluster.clusterNumber * firstClusterSize
                }
            }
        } else {
            it = cursor.getSearchKeyRange(ClusterKey.toByteIterable(fd, 0L))
            if (it == null) {
                current = null
            } else {
                val maxClusterSize = cs.maxClusterSize
                var clusterSize = 0L
                current = readCluster(it)
                var startingPosition = 0L
                adjustCurrentCluster()
                while (current != null) {
                    // if cluster size is equal to max cluster size, then all further cluster will have that size,
                    // so we don't need to load their size
                    if (clusterSize < maxClusterSize) {
                        clusterSize = current.notNull.getSize().toLong()
                    }
                    current!!.startingPosition = startingPosition
                    if (pos < clusterSize) {
                        break
                    }
                    pos -= clusterSize
                    startingPosition += clusterSize
                    moveToNext()
                }
            }
        }
    }

    fun hasCluster() = current != null

    fun moveToNext() {
        if (current != null) {
            if (!cursor.next) {
                current = null
            } else {
                current = readCluster(cursor.value)
                adjustCurrentCluster()
            }
        }
    }

    fun deleteCurrent() {
        if (current != null) {
            cursor.deleteCurrent()
        }
    }

    override fun close() {
        if (!isClosed) {
            cursor.close()
            isClosed = true
        }
    }

    private fun readCluster(it: ByteIterable): Cluster {
        val clusterConverter = vfs.clusterConverter
        return Cluster(clusterConverter?.onRead(it) ?: it)
    }

    private fun adjustCurrentCluster() {
        val clusterKey = ClusterKey(cursor.key)
        if (clusterKey.descriptor != fd) {
            current = null
        } else {
            val cancellingPolicyProvider = vfs.cancellingPolicyProvider
            if (cancellingPolicyProvider != null) {
                val cancellingPolicy = cancellingPolicyProvider.policy
                if (cancellingPolicy.needToCancel()) {
                    cancellingPolicy.doCancel()
                }
            }
            current.notNull.clusterNumber = clusterKey.clusterNumber
        }
    }
}