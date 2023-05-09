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
package jetbrains.exodus.log

import jetbrains.exodus.ByteIterable

/**
 * Loggable for writing
 */
class TestLoggable(private val type: Byte, private val data: ByteIterable, private val structureId: Int) : Loggable {

    override fun getType(): Byte = type

    override fun getData(): ByteIterable = data

    override fun getStructureId(): Int = structureId

    override fun getAddress(): Long {
        throw UnsupportedOperationException("TestLoggable has no address until it is written to log")
    }

    override fun length(): Int {
        throw UnsupportedOperationException("TestLoggable has no address until it is written to log")
    }

    override fun end(): Long {
        throw UnsupportedOperationException("TestLoggable has no address until it is written to log")
    }

    override fun getDataLength(): Int = data.length
    override fun isDataInsideSinglePage(): Boolean = true
}
