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

import jetbrains.exodus.log.DataCorruptionException
import jetbrains.exodus.vfs.File
import jetbrains.exodus.vfs.VfsInputStream
import org.apache.lucene.store.BufferedIndexInput
import java.io.IOException

internal class ExodusIndexInput(private val directory: ExodusDirectory,
                                name: String,
                                private var currentPosition: Long = 0L) : BufferedIndexInput("ExodusIndexInput[$name]") {

    private val file: File = directory.openExistingFile(name)
    private var input: VfsInputStream? = null

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

    override fun seekInternal(pos: Long) {
        if (pos != currentPosition) {
            if (pos > currentPosition) {
                val clusteringStrategy = directory.vfs.config.clusteringStrategy
                val bytesToSkip = pos - currentPosition
                val clusterSize = clusteringStrategy.firstClusterSize
                if ((!clusteringStrategy.isLinear || currentPosition % clusterSize + bytesToSkip < clusterSize) // or we are within single cluster
                        && getInput().skip(bytesToSkip) == bytesToSkip) {
                    currentPosition = pos
                    return
                }
            }
            close()
            currentPosition = pos
            getInput()
        }
    }

    override fun length() = directory.vfs.getFileLength(directory.environment.andCheckCurrentTransaction, file)

    override fun clone() = ExodusIndexInput(directory, file.path, currentPosition)

    private fun getInput(): VfsInputStream = input.let {
        if (it == null || it.isObsolete) {
            return@let directory.vfs.readFile(directory.environment.andCheckCurrentTransaction, file, currentPosition).apply {
                input = this
            }
        }
        return@let it
    }

    private fun handleFalseDataCorruption(e: DataCorruptionException) {
        // we use this dummy synchronized statement, since we don't want TransactionBase.isFinished to be a volatile field
        synchronized(directory) {
            if (input?.isObsolete != true) {
                throw e
            }
        }
    }
}
