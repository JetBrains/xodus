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

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.log.ByteIterableWithAddress
import jetbrains.exodus.log.ByteIteratorWithAddress
import jetbrains.exodus.tree.Dumpable
import jetbrains.exodus.tree.INode
import java.io.PrintStream
import java.util.NoSuchElementException
import kotlin.math.abs

internal abstract class NodeBase : INode {
    @JvmField
    internal var key: ByteIterable

    @JvmField
    internal var value: ByteIterable?

    constructor(keySequence: ByteIterable, value: ByteIterable?) {
        key = keySequence
        this.value = value
    }

    constructor(
        type: Byte,
        data: ByteIterableWithAddress,
        it: ByteIteratorWithAddress
    ) {
        key = extractKey(type, data, it)
        value = extractValue(type, data, it)
    }

    override fun getKey(): ByteIterable = key

    override fun getValue(): ByteIterable? = value

    fun matchesKeySequence(it: ByteIterator): Long {
        var matchingLength = 0
        val keyIt = key.iterator()
        if (keyIt is ArrayByteIterable.Iterator && it is ArrayByteIterable.Iterator) {
            matchingLength = keyIt.match(it)
            if (!keyIt.hasNext()) {
                return MatchResult.getMatchResult(matchingLength)
            }
            val keyByte = keyIt.next()
            if (!it.hasNext()) {
                return MatchResult.getMatchResult(-matchingLength - 1, keyByte, false, 0.toByte())
            }
            val nextByte = it.next()
            return MatchResult.getMatchResult(-matchingLength - 1, keyByte, true, nextByte)
        }
        while (keyIt.hasNext()) {
            val keyByte = keyIt.next()
            if (!it.hasNext()) {
                return MatchResult.getMatchResult(-matchingLength - 1, keyByte, false, 0.toByte())
            }
            val nextByte = it.next()
            if (nextByte != keyByte) {
                return MatchResult.getMatchResult(-matchingLength - 1, keyByte, true, nextByte)
            }
            ++matchingLength
        }
        return MatchResult.getMatchResult(matchingLength)
    }

    fun hasKey(): Boolean {
        return key !== ByteIterable.EMPTY && key.length > 0
    }

    override fun hasValue(): Boolean {
        return value != null
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        throw UnsupportedOperationException()
    }

    abstract fun getAddress(): Long
    abstract fun isMutable(): Boolean
    abstract fun getMutableCopy(mutableTree: PatriciaTreeMutable): MutableNode
    abstract fun getChild(tree: PatriciaTreeBase, b: Byte): NodeBase?
    abstract fun getChildren(b: Byte): NodeChildrenIterator
    abstract fun getChildrenRange(b: Byte): NodeChildrenIterator
    abstract fun getChildrenLast(): NodeChildrenIterator
    abstract fun getChildren(): NodeChildren
    abstract fun getChildrenCount(): Int
    override fun toString(): String {
        return String.format(
            "%s} %s %s",
            if (key.iterator().hasNext()) "{key:$key" else '{',
            if (value == null) "@" else value.toString() + " @", getAddress()
        )
    }

    internal object MatchResult {
        fun getMatchResult(matchingLength: Int): Long {
            return getMatchResult(matchingLength, 0.toByte(), false, 0.toByte())
        }

        fun getMatchResult(
            matchingLength: Int,
            keyByte: Byte,
            hasNext: Boolean,
            nextByte: Byte
        ): Long {
            var result = (abs(matchingLength)
                .toLong() shl 18) + (keyByte.toInt() and 0xff shl 10) + (nextByte.toInt() and 0xff shl 2)
            if (matchingLength < 0) {
                result += 2
            }
            if (hasNext) {
                ++result
            }
            return result
        }

        fun getMatchingLength(matchResult: Long): Int {
            val result = (matchResult shr 18).toInt()
            return if (matchResult and 2L == 0L) result else -result
        }

        fun getKeyByte(matchResult: Long): Int {
            return (matchResult shr 10).toInt() and 0xff
        }

        fun getNextByte(matchResult: Long): Int {
            return (matchResult shr 2).toInt() and 0xff
        }

        fun hasNext(matchResult: Long): Boolean {
            return matchResult and 1L != 0L
        }
    }

    internal inner class EmptyNodeChildrenIterator : NodeChildrenIterator {
        override fun getKey(): ByteIterable? = ByteIterable.EMPTY
        override fun isMutable(): Boolean = false

        override fun nextInPlace() {
            throw UnsupportedOperationException()
        }

        override fun prevInPlace() {
            throw UnsupportedOperationException()
        }

        override fun getNode(): ChildReference? = null
        override fun getParentNode(): NodeBase = this@NodeBase
        override fun getIndex(): Int = 0

        override fun hasNext(): Boolean {
            return false
        }

        override fun next(): ChildReference {
            throw NoSuchElementException()
        }

        override fun hasPrev(): Boolean {
            return false
        }

        override fun prev(): ChildReference? {
            return null
        }

        override fun remove() {}
    }

    companion object {
        fun indent(out: PrintStream, level: Int) {
            for (i in 0 until level) {
                out.print(' ')
            }
        }

        private fun extractKey(
            type: Byte,
            data: ByteIterableWithAddress,
            it: ByteIteratorWithAddress
        ): ByteIterable {
            return if (!PatriciaTreeBase.nodeHasKey(type)) {
                ByteIterable.EMPTY
            } else extractLazyIterable(data, it)
        }

        private fun extractValue(
            type: Byte,
            data: ByteIterableWithAddress,
            it: ByteIteratorWithAddress
        ): ByteIterable? {
            return if (!PatriciaTreeBase.nodeHasValue(type)) {
                null
            } else extractLazyIterable(data, it)
        }

        private fun extractLazyIterable(
            data: ByteIterableWithAddress,
            it: ByteIteratorWithAddress
        ): ByteIterable {
            val length = it.getCompressedUnsignedInt()
            if (length == 1) {
                return ArrayByteIterable.fromByte(it.next())
            }
            val result = data.cloneWithAddressAndLength(it.getAddress(), length)
            it.skip(length.toLong())
            return result
        }
    }
}