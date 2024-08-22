/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.io.inMemory

import jetbrains.exodus.ExodusException
import jetbrains.exodus.io.AbstractDataWriter
import jetbrains.exodus.io.Block
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.LogUtil
import mu.KLogging
import org.jetbrains.annotations.NotNull

open class MemoryDataWriter(private val memory: Memory) : AbstractDataWriter() {

    companion object : KLogging()

    private var closed = false
    private lateinit var data: Memory.Block

    override fun write(b: ByteArray, off: Int, len: Int): Block {
        checkClosed()
        data.write(b, off, len)
        return data
    }

    override fun removeBlock(blockAddress: Long, @NotNull rbt: RemoveBlockType) {
        if (!memory.removeBlock(blockAddress)) {
            throw ExodusException("There is no memory block by address $blockAddress")
        }
        logger.info { "Deleted file " + LogUtil.getLogFilename(blockAddress) }
    }

    override fun truncateBlock(blockAddress: Long, length: Long) {
        memory.getOrCreateBlockData(blockAddress, length)
        logger.info { "Truncated file " + LogUtil.getLogFilename(blockAddress) }
    }

    override fun lock(timeout: Long): Boolean {
        return true
    }

    override fun release() = true

    override fun lockInfo(): String? = null

    override fun syncImpl() {}

    override fun closeImpl() {
        closed = true
    }

    override fun clearImpl() = memory.clear()

    override fun openOrCreateBlockImpl(address: Long, length: Long): Block {
        val result = memory.getOrCreateBlockData(address, length)
        data = result
        closed = false
        return result
    }

    private fun checkClosed() {
        if (closed) {
            throw IllegalStateException("Already closed")
        }
    }
}
