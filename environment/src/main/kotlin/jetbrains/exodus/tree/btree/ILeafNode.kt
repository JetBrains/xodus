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
    val address: Long
    fun compareKeyTo(iterable: ByteIterable): Int
    fun compareValueTo(iterable: ByteIterable): Int
    fun valueExists(value: ByteIterable): Boolean
    fun addressIterator(): LongIterator
    val isDup: Boolean
    val isMutable: Boolean
    val tree: BTreeBase
    val dupCount: Long
    val isDupLeaf: Boolean

    companion object {
        val EMPTY: ILeafNode = object : ILeafNode {
            override fun hasValue(): Boolean {
                return false
            }

            override val key: ByteIterable
                get() = ByteIterable.EMPTY
            override val value: ByteIterable
                get() = ByteIterable.EMPTY
            override val address: Long
                get() = Loggable.NULL_ADDRESS

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

            override val isDup: Boolean
                get() = false
            override val isMutable: Boolean
                get() = false
            override val tree: BTreeBase
                get() {
                    throw UnsupportedOperationException()
                }
            override val dupCount: Long
                get() = 0
            override val isDupLeaf: Boolean
                get() = false

            override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
                out.println("Empty leaf node")
            }
        }
    }
}
