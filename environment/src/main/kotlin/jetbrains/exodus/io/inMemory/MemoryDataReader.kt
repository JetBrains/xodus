/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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

import jetbrains.exodus.io.Block
import jetbrains.exodus.io.DataReader
import mu.KLogging

open class MemoryDataReader(private val memory: Memory) : DataReader, KLogging() {

    private val memoryBlocks get() = memory.allBlocks.asSequence()

    override fun getBlocks(): Iterable<Block> {
        return memoryBlocks.sortedBy {
            it.address
        }.map { MemoryBlock(it) }.asIterable()
    }

    override fun getBlocks(fromAddress: Long): Iterable<Block> {
        return memoryBlocks.filter {
            it.address >= fromAddress
        }.sortedBy {
            it.address
        }.map { MemoryBlock(it) }.asIterable()
    }

    override fun close() {
        // nothing to do
    }

    override fun getLocation() = memory.toString()

    private class MemoryBlock constructor(private val data: Memory.Block) : Block {

        override fun getAddress(): Long {
            return data.address
        }

        override fun length() = data.length()

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int) =
                data.read(output, position, offset, count)

        override fun refresh() = this
    }
}
