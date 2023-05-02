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
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.log.ByteIterableWithAddress
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.tree.Dumpable
import java.io.PrintStream
import kotlin.math.max

internal class InternalPage : BasePageImmutable {
    private var childAddressLen: Byte = 0

    constructor(
        tree: BTreeBase, data: ByteIterableWithAddress,
        loggableInsideSinglePage: Boolean
    ) : super(tree, data, loggableInsideSinglePage)

    constructor(
        tree: BTreeBase, data: ByteIterableWithAddress, size: Int,
        loggableInsideSinglePage: Boolean
    ) : super(tree, data, size, loggableInsideSinglePage)

    override fun loadAddressLengths(length: Int, it: ByteIterator) {
        super.loadAddressLengths(length, it)
        it.skip(size.toLong() * keyAddressLen)
        checkAddressLength(it.next().also { childAddressLen = it })
    }

    override fun getMutableCopy(treeMutable: BTreeMutable): BasePageMutable {
        return InternalPageMutable(treeMutable, this)
    }

    override fun getChildAddress(index: Int): Long {
        val offset = size * keyAddressLen + 1 + index * childAddressLen
        return if (page != null) {
            getLong(offset, childAddressLen.toInt())
        } else data.nextLong(offset, childAddressLen.toInt())
    }

    override fun getChild(index: Int): BasePage {
        return tree.loadPage(getChildAddress(index))
    }

    override val isBottom: Boolean
        get() = false

    override fun get(key: ByteIterable): ILeafNode? {
        return Companion[key, this]
    }

    override fun find(
        stack: BTreeTraverser,
        depth: Int,
        key: ByteIterable,
        value: ByteIterable?,
        equalOrNext: Boolean
    ): ILeafNode? {
        return find(stack, depth, key, value, equalOrNext, this)
    }

    override fun keyExists(key: ByteIterable): Boolean {
        return keyExists(key, this)
    }

    override fun exists(key: ByteIterable, value: ByteIterable): Boolean {
        return exists(key, value, this)
    }

    override fun childExists(key: ByteIterable, pageAddress: Long): Boolean {
        val index = binarySearchGuessUnsafe(this, key)
        return index >= 0 && (getChildAddress(index) == pageAddress || getChild(index).childExists(key, pageAddress))
    }

    override val bottomPagesCount: Long
        get() {
            var result: Long = 0
            for (i in 0 until size) {
                result += getChild(i).bottomPagesCount
            }
            return result
        }

    override fun toString(): String {
        return "Internal [$size] @ " + (dataAddress -
                getIterable((size shl 1).toLong()).length - 2)
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        dump(out, level, renderer, this)
    }

    fun reclaim(keyIterable: ByteIterable?, context: BTreeReclaimTraverser) {
        if (context.currentNode.dataAddress != dataAddress) {
            // go up
            if (context.canMoveUp()) {
                while (true) {
                    context.popAndMutate()
                    if (context.currentNode.dataAddress == dataAddress) {
                        doReclaim(context)
                        return
                    }
                    context.moveRight()
                    val index = context.getNextSibling(keyIterable!!)
                    if (index < 0) {
                        if (!context.canMoveUp()) {
                            context.moveTo((-index - 2).coerceAtLeast(0))
                            break
                        }
                    } else {
                        context.pushChild(index) // node is always internal
                        break
                    }
                }
            }
            // go down
            while (context.canMoveDown()) {
                if (context.currentNode.dataAddress == dataAddress) {
                    doReclaim(context)
                    return
                }
                var index = context.getNextSibling(keyIterable!!)
                if (index < 0) {
                    index = max(-index - 2, 0)
                }
                context.pushChild(index)
            }
        } else {
            doReclaim(context)
        }
    }

    companion object {
        operator fun get(key: ByteIterable, page: BasePage): ILeafNode? {
            val index = page.binarySearch(key)
            return if (index < 0) page.getChild(max(-index - 2, 0))[key] else page.getKey(index)
        }

        fun find(
            stack: BTreeTraverser,
            depth: Int,
            key: ByteIterable,
            value: ByteIterable?,
            equalOrNext: Boolean,
            page: BasePage
        ): ILeafNode? {
            var index = binarySearchGuess(page, key)
            var ln = page.getChild(index).find(stack, depth + 1, key, value, equalOrNext)
            if (ln == null && value == null && equalOrNext) {
                // try next child
                if (index < page.size - 1) {
                    ++index
                    ln = page.getChild(index).find(stack, depth + 1, key, null, true)
                }
            }
            if (ln != null) {
                stack.setAt(depth, TreePos(page, index))
            }
            return ln
        }

        fun keyExists(key: ByteIterable, page: BasePage): Boolean {
            val index = page.binarySearch(key)
            return index >= 0 || page.getChild(max(-index - 2, 0)).keyExists(key)
        }

        fun exists(key: ByteIterable, value: ByteIterable, page: BasePage): Boolean {
            val index = page.binarySearch(key)
            return if (index < 0) page.getChild(max(-index - 2, 0)).exists(key, value) else page.getKey(index)
                .valueExists(value)
        }

        /*
     * Returns safe binary search.
     * @return index (non-negative result is guaranteed)
     */
        fun binarySearchGuess(page: BasePage, key: ByteIterable): Int {
            var index = binarySearchGuessUnsafe(page, key)
            if (index < 0) index = 0
            return index
        }

        /*
     * Returns unsafe binary search index.
     * @return index (non-negative or -1 which means that nothing was found)
     */
        fun binarySearchGuessUnsafe(page: BasePage, key: ByteIterable): Int {
            var index = page.binarySearch(key)
            if (index < 0) {
                index = -index - 2
            }
            return index
        }

        fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?, page: BasePage) {
            indent(out, level)
            out.println(page)
            for (i in 0 until page.size) {
                indent(out, level)
                out.print("+")
                page.getKey(i).dump(out, 0, renderer)
                page.getChild(i).dump(out, level + 3, renderer)
            }
        }
    }
}
