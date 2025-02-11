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
package jetbrains.exodus.env

class ContextualBitmapImpl(override val store: ContextualStoreImpl) : ContextualBitmap, BitmapImpl(store) {

    override fun getEnvironment() = store.environment

    override fun get(bit: Long): Boolean = get(currentTransaction, bit)

    override fun set(bit: Long, value: Boolean) = set(currentTransaction, bit, value)

    override fun clear(bit: Long) = clear(currentTransaction, bit)

    override fun iterator() = iterator(currentTransaction)

    override fun reverseIterator() = reverseIterator(currentTransaction)

    override fun getFirst(): Long = getFirst(currentTransaction)

    override fun getLast(): Long = getLast(currentTransaction)

    override fun count() = count(currentTransaction)

    override fun count(firstBit: Long, lastBit: Long) = count(currentTransaction, firstBit, lastBit)

    private val currentTransaction: Transaction get() = environment.andCheckCurrentTransaction


}