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
import jetbrains.exodus.tree.Dumpable
import java.io.PrintStream

/**
 * BTree base implementation of page
 */
abstract class BasePage protected constructor(val tree: BTreeBase) : Dumpable {
    @JvmField
    var size = 0

    open fun getChild(index: Int): BasePage {
        throw UnsupportedOperationException()
    }

    open val minKey: ILeafNode
        get() {
            if (size <= 0) {
                throw ArrayIndexOutOfBoundsException("Page is empty.")
            }
            return getKey(0)
        }
    open val maxKey: ILeafNode
        get() {
            if (size <= 0) {
                throw ArrayIndexOutOfBoundsException("Page is empty.")
            }
            return getKey(size - 1)
        }

    fun isInPageRange(key: ByteIterable, value: ByteIterable?): Boolean {
        val maxKey = maxKey
        var cmp = maxKey.key.compareTo(key)
        if (cmp < 0) {
            return false
        }
        if (cmp == 0 && value != null) {
            val maxValue = maxKey.value
            if (maxValue == null || maxValue < value) {
                return false
            }
        }
        if (size == 1) {
            return true
        }
        val minKey = minKey
        cmp = minKey.key.compareTo(key)
        if (cmp > 0) {
            return false
        }
        if (cmp == 0 && value != null) {
            val minValue = minKey.value
            return minValue != null && minValue <= value
        }
        return true
    }

    open fun isDupKey(index: Int): Boolean {
        return getKey(index).isDup
    }

    abstract fun getKey(index: Int): BaseLeafNode
    abstract fun getMutableCopy(treeMutable: BTreeMutable): BasePageMutable
    abstract val dataAddress: Long
    abstract fun getKeyAddress(index: Int): Long
    abstract val isBottom: Boolean
    abstract val isMutable: Boolean
    abstract val bottomPagesCount: Long
    abstract fun binarySearch(key: ByteIterable): Int
    abstract fun binarySearch(key: ByteIterable, low: Int): Int
    abstract fun binarySearch(key: ByteIterable, low: Int, expectedAddress: Long): Int
    abstract operator fun get(key: ByteIterable): ILeafNode?
    abstract fun find(
        stack: BTreeTraverser, depth: Int,
        key: ByteIterable, value: ByteIterable?, equalOrNext: Boolean
    ): ILeafNode?

    abstract fun keyExists(key: ByteIterable): Boolean
    abstract fun exists(key: ByteIterable, value: ByteIterable): Boolean
    abstract fun childExists(key: ByteIterable, pageAddress: Long): Boolean
    abstract fun getChildAddress(index: Int): Long

    companion object {
        fun indent(out: PrintStream, level: Int) {
            for (i in 0 until level) out.print(" ")
        }
    }
}
