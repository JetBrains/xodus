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
package jetbrains.exodus.io

abstract class AbstractDataWriter protected constructor() : DataWriter {
    private var open = false
    override fun isOpen(): Boolean {
        return open
    }

    override fun sync() {
        if (open) {
            syncImpl()
        }
    }

    override fun syncDirectory() {}
    override fun close() {
        if (open) {
            closeImpl()
            open = false
        }
    }

    override fun clear() {
        close()
        clearImpl()
    }

    override fun openOrCreateBlock(address: Long, length: Long): Block {
        return if (open) {
            throw IllegalStateException("Can't create blocks with open data writer")
        } else {
            val result = openOrCreateBlockImpl(address, length)
            open = true
            result
        }
    }

    protected abstract fun syncImpl()
    protected abstract fun closeImpl()
    protected abstract fun clearImpl()
    protected abstract fun openOrCreateBlockImpl(address: Long, length: Long): Block
}
