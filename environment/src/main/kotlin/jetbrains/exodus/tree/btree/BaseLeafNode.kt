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

import jetbrains.exodus.*
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.Dumpable
import jetbrains.exodus.tree.INode
import jetbrains.exodus.tree.LongIterator
import java.io.PrintStream

/**
 * Base implementation of leaf node
 */
abstract class BaseLeafNode : ILeafNode {
    override fun equals(other: Any?): Boolean {
        return this === other || other is INode && key == other.key
    }

    override fun hasValue(): Boolean {
        return true
    }

    override val isDupLeaf: Boolean
        get() = false

    override fun hashCode(): Int {
        return key.hashCode()
    }

    override val address: Long
        get() = Loggable.NULL_ADDRESS
    override val tree: BTreeBase
        get() {
            throw UnsupportedOperationException()
        }

    override fun addressIterator(): LongIterator {
        return LongIterator.EMPTY
    }

    override val isDup: Boolean
        get() = false
    override val dupCount: Long
        get() = 1

    override fun valueExists(value: ByteIterable): Boolean {
        return compareValueTo(value) == 0
    }

    override fun compareKeyTo(iterable: ByteIterable): Int {
        return key.compareTo(iterable)
    }

    override fun compareValueTo(iterable: ByteIterable): Int {
        return value!!.compareTo(iterable)
    }

    override fun toString(): String {
        return "LN {key:$key} @ $address"
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        BasePage.indent(out, level)
        out.println(if (renderer == null) toString() else renderer.toString(this))
    }
}
