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

/**
 * Stops at the end of log or on the file hole
 */
class LoggableIterator(private val log: Log, private val it: ByteIteratorWithAddress, private val highAddress: Long) :
    MutableIterator<RandomAccessLoggable> {
    constructor(log: Log, startAddress: Long, highAddress: Long) : this(
        log,
        log.readIteratorFrom(startAddress),
        highAddress
    )

    fun getHighAddress(): Long {
        return it.address
    }

    override fun next(): RandomAccessLoggable {
        if (!hasNext()) {
            throw NoSuchElementException()
        }

        val result = log.read(it)
        if (!NullLoggable.isNullLoggable(result) || !HashCodeLoggable.isHashCodeLoggable(result)) {
            it.skip(result.getDataLength().toLong())
        }
        return result
    }

    override fun hasNext(): Boolean {
        return it.hasNext() && it.address < highAddress
    }

    override fun remove() {}
}
