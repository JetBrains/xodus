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
package jetbrains.exodus.io.inMemory

import jetbrains.exodus.ExodusException
import jetbrains.exodus.io.Block
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogUtil
import mu.KLogging
import org.jetbrains.annotations.NotNull

open class MemoryDataReader(private val memory: Memory) : DataReader {
    companion object : KLogging()

    private val memoryBlocks get() = memory.allBlocks.asSequence()

    override fun getBlocks(): Iterable<Block> {
        return memoryBlocks.sortedBy {
            it.address
        }.map { MemoryBlock(it) }.asIterable()
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

    override fun clear() {
        memory.clear()
    }

    override fun close() {
        // nothing to do
    }

    override fun setLog(@NotNull log: Log) {
        // we don't need Log here
    }

    override fun getLocation(): String {
        return memory.toString()
    }

    override fun getBlock(address: Long): Block {
        return MemoryBlock(memory.getBlockData(address))
    }

    private class MemoryBlock constructor(private val data: Memory.Block) : Block {

        override fun getAddress(): Long {
            return data.address
        }

        override fun length(): Long {
            return data.size.toLong()
        }

        override fun read(output: ByteArray, position: Long, count: Int): Int {
            return data.read(output, position, count)
        }

        override fun setReadOnly(): Boolean {
            return false
        }
    }
}
