/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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

import jetbrains.exodus.env.Transaction
import jetbrains.exodus.log.DataCorruptionException
import jetbrains.exodus.vfs.File
import jetbrains.exodus.vfs.VfsInputStream
import org.apache.lucene.store.BufferedIndexInput
import org.apache.lucene.store.IndexInput
import java.io.IOException
import kotlin.math.max
import kotlin.math.min

internal const val BUFFER_SIZE = 512
internal const val MERGE_BUFFER_SIZE = 4096

internal open class ExodusIndexInput(private val directory: ExodusDirectory,
                                     private val file: File,
                                     bufferSize: Int) :
        BufferedIndexInput("ExodusIndexInput[${file.path}]", bufferSize) {

    private var input: VfsInputStream? = null
    private var currentPosition: Long = 0L
    private var cachedLength: Pair<Transaction?, Long> = null to -1L

    override fun length(): Long {
        val tx = txn
        if (tx.isReadonly) {
            val (cachedTxn, cachedLen) = cachedLength
            if (cachedTxn === tx) {
                return cachedLen
            }
            return directory.vfs.getFileLength(tx, file).also { len -> cachedLength = tx to len }
        }
        return directory.vfs.getFileLength(tx, file)
    }

    override fun clone() = filePointer.let {
        // do seek() in order to force invocation of refill() in cloned IndexInput
        ExodusIndexInput(directory, file, bufferSize).apply { seek(it) }
    }

    override fun close() {
        input?.apply {
            close()
            input = null
        }
    }

    @Throws(IOException::class)
    override fun readInternal(b: ByteArray, offset: Int, length: Int) {
        while (true) {
            try {
                currentPosition += getInput().read(b, offset, length).toLong()
                return
            } catch (e: DataCorruptionException) {
                handleFalseDataCorruption(e)
            }

        }
    }

    @Throws(IOException::class)
    override fun seekInternal(pos: Long) {
        if (pos != currentPosition) {
            val input = input
            if (input == null) {
                currentPosition = pos
                return
            }
            if (pos > currentPosition) {
                val clusteringStrategy = directory.vfs.config.clusteringStrategy
                val bytesToSkip = pos - currentPosition
                val clusterSize = clusteringStrategy.firstClusterSize
                if ((!clusteringStrategy.isLinear || currentPosition % clusterSize + bytesToSkip < clusterSize) // or we are within single cluster
                        && input.skip(bytesToSkip) == bytesToSkip) {
                    currentPosition = pos
                    return
                }
            }
            close()
            currentPosition = pos
        }
    }

    override fun slice(sliceDescription: String, offset: Long, length: Long): IndexInput =
            SlicedExodusIndexInput(this, offset, length)

    private fun getInput(): VfsInputStream = input.let {
        if (it == null || it.isObsolete) {
            return@let directory.vfs.readFile(txn, file, currentPosition).apply { input = this }
        }
        it
    }

    private val txn: Transaction get() = directory.environment.andCheckCurrentTransaction

    private fun handleFalseDataCorruption(e: DataCorruptionException) {
        // we use this dummy synchronized statement, since we don't want TransactionBase.isFinished to be a volatile field
        synchronized(directory) {
            if (input?.isObsolete != true) {
                throw e
            }
        }
    }

    private class SlicedExodusIndexInput(private val base: ExodusIndexInput,
                                         private val fileOffset: Long,
                                         private val length: Long) :
            ExodusIndexInput(base.directory, base.file, max(min(BUFFER_SIZE.toLong(), length).toInt(), MIN_BUFFER_SIZE)) {

        override fun length() = length

        override fun clone() = filePointer.let {
            SlicedExodusIndexInput(base, fileOffset, length).apply { seek(it) }
        }

        override fun seekInternal(pos: Long) = super.seekInternal(pos + fileOffset)

        override fun slice(sliceDescription: String, offset: Long, length: Long) =
                base.slice(sliceDescription, fileOffset + offset, length)
    }
}