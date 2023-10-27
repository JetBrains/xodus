/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.hash.LongIterator

internal fun skipTail(source: LongIterator, key: Long): LongIterator = SkipLessThanLongIterator(source, key)

private class SkipLessThanLongIterator(private val source: LongIterator, private val key: Long) : LongIterator {

    private var next: Long? = fetchNext()

    override fun hasNext() = next != null

    override fun nextLong() = (next ?: throw NoSuchElementException()).also { next = fetchNext() }

    override fun next() = nextLong()

    override fun remove() = throw UnsupportedOperationException()

    fun fetchNext(): Long? {
        while (source.hasNext()) {
            val next = source.nextLong()
            if (next >= key) {
                return next
            }
        }
        return null
    }
}