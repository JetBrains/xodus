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

import jetbrains.exodus.io.AbstractDataWriter

open class MemoryDataWriter(private val memory: Memory) : AbstractDataWriter() {

    private var closed = false
    private lateinit var data: Memory.Block

    override fun write(b: ByteArray, off: Int, len: Int) {
        checkClosed()
        data.write(b, off, len)
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

    override fun openOrCreateBlockImpl(address: Long, length: Long) {
        data = memory.getOrCreateBlockData(address, length)
        closed = false
    }

    private fun checkClosed() {
        if (closed) {
            throw IllegalStateException("Already closed")
        }
    }
}
