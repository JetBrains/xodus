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
package jetbrains.exodus.tree.patricia

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.log.DataIterator
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.tree.Dumpable
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.LongIterator
import java.io.PrintStream

abstract class PatriciaTreeBase protected constructor(final override val log: Log, structureId: Int) : ITree {
    private val dataIterator: DataIterator = DataIterator(log)
    final override val structureId: Int
    final override var size: Long = 0
        protected set

    init {
        this.structureId = structureId
    }

    override fun getDataIterator(address: Long): DataIterator {
        dataIterator.checkPage(address)
        return dataIterator
    }

    override fun get(key: ByteIterable): ByteIterable? {
        val node = getNode(key)
        return node?.value
    }

    override fun hasPair(key: ByteIterable, value: ByteIterable): Boolean {
        val `val` = get(key)
        return `val` != null && `val`.compareTo(value) == 0
    }

    override fun hasKey(key: ByteIterable): Boolean {
        return get(key) != null
    }

    override val isEmpty: Boolean
        get() = size == 0L

    override fun dump(out: PrintStream) {
        dump(out, null)
    }

    override fun dump(out: PrintStream, renderer: Dumpable.ToString?) {
        TreeAwareNodeDecorator(this, this.root!!).dump(out, 0, renderer)
    }

    override fun addressIterator(): LongIterator {
        return if (isEmpty) {
            LongIterator.EMPTY
        } else AddressIterator(PatriciaTraverser(this, this.root!!))
    }

    fun getLoggable(address: Long): RandomAccessLoggable {
        return log.readNotNull(getDataIterator(address), address)
    }

    fun loadNode(address: Long): NodeBase {
        val loggable = getLoggable(address)
        return if (loggable.isDataInsideSinglePage) {
            SinglePageImmutableNode(loggable, loggable.data)
        } else MultiPageImmutableNode(log, loggable, loggable.data)
    }

    abstract val root: NodeBase?
    protected open fun getNode(key: ByteIterable): NodeBase? {
        val it = key.iterator()
        var node = this.root
        do {
            if (NodeBase.MatchResult.getMatchingLength(node!!.matchesKeySequence(it)) < 0) {
                return null
            }
            if (!it.hasNext()) {
                break
            }
            node = node.getChild(this, it.next())
        } while (node != null)
        return node
    }

    companion object {
        /**
         * Loggable types describing patricia tree nodes.
         * All types start from the NODE_WO_KEY_WO_VALUE_WO_CHILDREN which corresponds to a non-root node without key,
         * without value, without children and without back reference. All other patricia loggables' types are made
         * using additional 5 bits, giving additional 31 types. So maximum value of a patricia loggable type is 43.
         */
        const val MAX_VALID_LOGGABLE_TYPE: Byte = 43
        const val NODE_WO_KEY_WO_VALUE_WO_CHILDREN: Byte = 12
        const val HAS_KEY_BIT: Byte = 1
        const val HAS_VALUE_BIT: Byte = 2
        const val HAS_CHILDREN_BIT: Byte = 4
        const val ROOT_BIT: Byte = 8
        const val ROOT_BIT_WITH_BACKREF: Byte = 16
        fun nodeHasKey(type: Byte): Boolean {
            return type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN and HAS_KEY_BIT.toInt() != 0
        }

        fun nodeHasValue(type: Byte): Boolean {
            return type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN and HAS_VALUE_BIT.toInt() != 0
        }

        fun nodeHasChildren(type: Byte): Boolean {
            return type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN and HAS_CHILDREN_BIT.toInt() != 0
        }

        fun nodeIsRoot(type: Byte): Boolean {
            return type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN and ROOT_BIT.toInt() != 0
        }

        fun nodeHasBackReference(type: Byte): Boolean {
            return type - NODE_WO_KEY_WO_VALUE_WO_CHILDREN and ROOT_BIT_WITH_BACKREF.toInt() != 0
        }
    }
}
