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
package jetbrains.exodus.tree.btree

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.Dumpable
import jetbrains.exodus.tree.INode
import jetbrains.exodus.tree.LongIterator
import java.io.PrintStream

interface ILeafNode : INode {
    fun getAddress(): Long
    fun compareKeyTo(iterable: ByteIterable): Int
    fun compareValueTo(iterable: ByteIterable): Int
    fun valueExists(value: ByteIterable): Boolean
    fun addressIterator(): LongIterator
    fun isDup(): Boolean
    fun isMutable(): Boolean
    fun getTree(): BTreeBase
    fun getDupCount(): Long
    fun isDupLeaf(): Boolean

    companion object {
        val EMPTY: ILeafNode = object : ILeafNode {
            override fun hasValue(): Boolean {
                return false
            }

            override fun getKey(): ByteIterable = ByteIterable.EMPTY

            override fun getValue(): ByteIterable? = ByteIterable.EMPTY

            override fun getAddress(): Long = Loggable.NULL_ADDRESS

            override fun compareKeyTo(iterable: ByteIterable): Int {
                throw UnsupportedOperationException()
            }

            override fun compareValueTo(iterable: ByteIterable): Int {
                throw UnsupportedOperationException()
            }

            override fun valueExists(value: ByteIterable): Boolean {
                throw UnsupportedOperationException()
            }

            override fun addressIterator(): LongIterator {
                throw UnsupportedOperationException()
            }

            override fun isDup(): Boolean = false

            override fun isMutable(): Boolean = false

            override fun getTree(): BTreeBase = throw UnsupportedOperationException()

            override fun getDupCount(): Long = 0

            override fun isDupLeaf(): Boolean = false

            override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
                out.println("Empty leaf node")
            }
        }
    }
}
