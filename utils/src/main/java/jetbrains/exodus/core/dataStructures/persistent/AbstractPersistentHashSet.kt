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
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.Stack
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure
import java.util.*

abstract class AbstractPersistentHashSet<K> : Iterable<K> {
    abstract val root: RootTableNode<K>
    operator fun contains(key: K): Boolean {
        return getKey(key) != null
    }

    fun getKey(key: K): K? {
        return this.root.getKey(key, key.hashCode(), 0)
    }

    val isEmpty: Boolean
        get() = this.root.mask == 0

    fun size(): Int {
        return this.root.size
    }

    override fun iterator(): MutableIterator<K> {
        val root = this.root
        return if (root.mask == 0) Collections.EMPTY_LIST.iterator() else Itr(root)
    }

    internal class Itr<K>(startingRoot: Node<K>) : MutableIterator<K?> {
        private val startingRoot: Node<K?>
        private var stack: Stack<TreePos<K?>>? = null
        private var hasNext = false
        private var hasNextValid = false

        init {
            this.startingRoot = startingRoot
        }

        override fun hasNext(): Boolean {
            if (hasNextValid) {
                return hasNext
            }
            hasNextValid = true
            if (stack == null) {
                stack = Stack()
                val treePos = TreePos(startingRoot)
                treePos.index = -1
                stack!!.push(treePos)
            }
            var treePos = stack!!.peek()!!
            treePos.index++
            while (treePos.node.isOut(treePos.index)) {
                stack!!.pop()
                if (stack!!.isEmpty()) {
                    hasNext = false
                    return false
                }
                treePos = stack!!.peek()!!
                treePos.index++
            }
            while (true) {
                val o = treePos.node[treePos.index]
                if (o !is Node<*>) {
                    hasNext = true
                    return true
                }
                treePos = TreePos<Any?>(o as Node<K?>)
                stack!!.push(treePos)
            }
        }

        override fun next(): K? {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            hasNextValid = false
            val treePos = stack!!.peek()!!
            return treePos.node[treePos.index] as K?
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }

    interface Node<K> {
        fun insert(key: K, hash: Int, offset: Int, flag: Flag): Node<K>
        fun remove(key: K, hash: Int, offset: Int): Any?
        fun getKey(key: K, hash: Int, offset: Int): K?
        fun checkNode(offset: Int)
        fun asRoot(size: Int): RootTableNode<K>
        operator fun get(index: Int): Any?
        fun isOut(index: Int): Boolean
        fun forEachKey(procedure: ObjectProcedure<K>)
    }

    internal class TreePos<K>(val node: Node<K>) {
        val index = 0
    }

    class RootTableNode<K>(mask: Int, table: Array<Any?>, val size: Int) : TableNode<K>(mask, table)
    open class TableNode<K> : Node<K> {
        val mask: Int
        private val table: Array<Any?>

        constructor(mask: Int, table: Array<Any?>) {
            this.mask = mask
            this.table = table
        }

        private constructor(key1: K, hash1: Int, key2: K, hash2: Int, offset: Int) {
            val subhash2 = getSubhash(hash2, offset)
            val subhash1 = getSubhash(hash1, offset)
            if (subhash1 == subhash2) {
                mask = 1 shl subhash1
                table = arrayOf(createNode(key1, hash1, key2, hash2, offset + BITS_PER_TABLE))
            } else {
                mask = (1 shl subhash2) + (1 shl subhash1)
                table = if (subhash1 < subhash2) {
                    arrayOf(key1, key2)
                } else {
                    arrayOf(key2, key1)
                }
            }
        }

        override fun insert(key: K, hash: Int, offset: Int, flag: Flag): Node<K> {
            val subhash = getSubhash(hash, offset)
            if (mask and (1 shl subhash) == 0) {
                flag.flag()
                return cloneAndAdd(key, subhash)
            }
            val index = getPosition(subhash)
            val target = table[index]
            val result: Any
            if (target is Node<*>) {
                result = (target as Node<K>).insert(key, hash, offset + BITS_PER_TABLE, flag)
            } else {
                // target is key
                if (target == key) {
                    result = key
                } else {
                    flag.flag()
                    result = createNode(target as K?, key, hash, offset + BITS_PER_TABLE)
                }
            }
            return cloneAndReplace(result, index, offset) as Node<K>
        }

        override fun remove(key: K, hash: Int, offset: Int): Any? {
            val subhash = getSubhash(hash, offset)
            if (mask and (1 shl subhash) == 0) {
                return null
            }
            val index = getPosition(subhash)
            val target = table[index]
            return if (target is Node<*>) {
                val removed =
                    (target as Node<K>).remove(
                        key,
                        hash,
                        offset + BITS_PER_TABLE
                    )
                        ?: return null
                cloneAndReplace(removed, index, offset)
            } else {
                if (target == key) cloneAndRemove(subhash, index, offset) else null
            }
        }

        override fun getKey(key: K, hash: Int, offset: Int): K {
            val subhash = getSubhash(hash, offset)
            if (mask and (1 shl subhash) == 0) {
                return null
            }
            val index = getPosition(subhash)
            val target = table[index]
            if (target is Node<*>) {
                return (target as Node<K>).getKey(key, hash, offset + BITS_PER_TABLE)
            }
            return if (target == key) target as K? else null
        }

        override fun forEachKey(procedure: ObjectProcedure<K>) {
            for (target in table) {
                if (target is Node<*>) {
                    (target as Node<K>).forEachKey(procedure)
                } else {
                    procedure.execute(target as K?)
                }
            }
        }

        private fun cloneAndAdd(key: K, subhash: Int): TableNode<K> {
            val index = getPosition(subhash)
            val tableLength = table.size
            val newTable = arrayOfNulls<Any>(tableLength + 1)
            System.arraycopy(table, 0, newTable, 0, index)
            System.arraycopy(table, index, newTable, index + 1, tableLength - index)
            newTable[index] = key
            return TableNode(mask + (1 shl subhash), newTable)
        }

        private fun cloneAndReplace(newChild: Any, index: Int, offset: Int): Any {
            val tableLength = table.size
            if (offset != 0 && tableLength == 1 && newChild !is Node<*>) {
                return newChild
            }
            val newTable = Arrays.copyOf(table, tableLength)
            newTable[index] = newChild
            return TableNode<K>(mask, newTable)
        }

        private fun cloneAndRemove(subhash: Int, index: Int, offset: Int): Any? {
            // mask & (1 << subhash) != 0
            val size = getPosition(1 shl BITS_PER_TABLE)
            if (size == 1) {
                return TableNode<K>(0, EMPTY_TABLE)
            }
            if (size == 2) {
                return if (offset == 0 || table[1 - index] is Node<*>) {
                    TableNode<Any?>(mask - (1 shl subhash), arrayOf(table[1 - index]))
                } else {
                    table[1 - index]
                }
            }
            val newTable = arrayOfNulls<Any>(table.size - 1)
            System.arraycopy(table, 0, newTable, 0, index)
            System.arraycopy(table, index + 1, newTable, index, newTable.size - index)
            return TableNode<Any?>(mask - (1 shl subhash), newTable)
        }

        /**
         * @param subhash amount of lowest bits to consider
         * @return amount of 1s in `mask` among `subhash` lowest bits
         */
        private fun getPosition(subhash: Int): Int {
            var m = mask and if (subhash == 32) -1 else (1 shl subhash) - 1
            m -= m ushr 1 and 0x55555555
            m = (m and 0x33333333) + (m ushr 2 and 0x33333333)
            m = m + (m ushr 4) and 0x0F0F0F0F
            m += m ushr 8
            return m + (m ushr 16) and 0xFF
        }

        override fun checkNode(offset: Int) {
            if (offset > 0) {
                val tableLength = table.size
                if (tableLength == 0 || tableLength == 1 && table[0] !is Node<*>) {
                    throw RuntimeException("unnecessary use of table node")
                }
            }
            var m = mask
            for (o in table) {
                if (m == 0) {
                    throw RuntimeException("Inconsistent mask and table")
                }
                m = m and m - 1
                if (o == null) {
                    throw RuntimeException("Null in table")
                }
                if (o is Node<*>) {
                    (o as Node<K>).checkNode(offset + BITS_PER_TABLE)
                }
            }
            if (m != 0) {
                throw RuntimeException("Inconsistent mask and table")
            }
        }

        override fun asRoot(size: Int): RootTableNode<K> {
            return RootTableNode(mask, table, size)
        }

        override fun get(index: Int): Any? {
            return table[index]
        }

        override fun isOut(index: Int): Boolean {
            return index + 1 > table.size
        }

        companion object {
            private fun <K> createNode(notHashedKey: K, hashedKey: K, hash: Int, offset: Int): Node<K> {
                return createNode(notHashedKey, notHashedKey.hashCode(), hashedKey, hash, offset)
            }

            private fun <K> createNode(key1: K, hash1: Int, key2: K, hash2: Int, offset: Int): Node<K> {
                val subhash1 = getSubhash(hash1, offset)
                val subhash2 = getSubhash(hash2, offset)
                val table: Array<Any?>
                table = if (subhash2 == subhash1) {
                    arrayOf(
                        if (offset + BITS_PER_TABLE >= BITS_IN_HASH) HashCollisionNode(
                            key1,
                            key2
                        ) else TableNode(key1, hash1, key2, hash2, offset + BITS_PER_TABLE)
                    )
                } else {
                    if (subhash2 < subhash1) arrayOf(key2, key1) else arrayOf<Any>(key1, key2)
                }
                return TableNode(1 shl subhash2 or (1 shl subhash1), table)
            }

            private fun getSubhash(hash: Int, offset: Int): Int {
                return hash ushr offset and (1 shl BITS_PER_TABLE) - 1
            }
        }
    }

    internal class HashCollisionNode<K>(vararg keys: K) : Node<K> {
        private val keys: Array<K>

        init {
            this.keys = keys
        }

        override fun insert(key: K, hash: Int, offset: Int, flag: Flag): Node<K> {
            val keysLength = keys.size
            for (i in 0 until keysLength) {
                if (keys[i] == key) {
                    val newKeys = keys.clone()
                    newKeys[i] = key
                    return HashCollisionNode(*newKeys)
                }
            }
            val newKeys = Arrays.copyOf(keys, keysLength + 1)
            newKeys[keysLength] = key
            flag.flag()
            return HashCollisionNode(*newKeys)
        }

        override fun remove(key: K, hash: Int, offset: Int): Any? {
            val keysLength = keys.size
            for (i in 0 until keysLength) {
                if (keys[i] == key) {
                    if (keysLength == 2) {
                        return keys[1 - i]
                    }
                    val newKeys = arrayOfNulls<Any>(keysLength - 1) as Array<K?>
                    var j = 0
                    var k = 0
                    while (j < keysLength) {
                        if (j != i) {
                            newKeys[k++] = keys[j]
                        }
                        ++j
                    }
                    return HashCollisionNode<K>(*newKeys)
                }
            }
            return null
        }

        override fun getKey(key: K, hash: Int, offset: Int): K {
            for (k in keys) {
                if (k == key) {
                    return k
                }
            }
            return null
        }

        override fun checkNode(offset: Int) {
            val keysLength = keys.size
            if (keysLength < 2) {
                throw RuntimeException("Unnecessary hash collision node of cardinality $keysLength")
            }
            for (key in keys) {
                if (key == null) {
                    throw RuntimeException("Null in collision list")
                }
            }
        }

        override fun asRoot(size: Int): RootTableNode<K> {
            throw UnsupportedOperationException("Unexpected as root!")
        }

        override fun get(index: Int): Any? {
            return keys[index]
        }

        override fun isOut(index: Int): Boolean {
            return index + 1 > keys.size
        }

        override fun forEachKey(procedure: ObjectProcedure<K>) {
            for (k in keys) {
                procedure.execute(k)
            }
        }
    }

    companion object {
        val EMPTY_TABLE = arrayOf<Any?>()
        val EMPTY_ROOT: RootTableNode<*> = RootTableNode<Any?>(0, EMPTY_TABLE, 0)
        const val BITS_PER_TABLE = 5
        const val BITS_IN_HASH = 32
    }
}
