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
import jetbrains.exodus.log.ByteIterableWithAddress
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.tree.Dumpable
import java.io.PrintStream
import kotlin.math.max

internal open class BottomPage : BasePageImmutable {
    constructor(tree: BTreeBase) : super(tree)
    constructor(
        tree: BTreeBase,
        data: ByteIterableWithAddress,
        loggableInsideSinglePage: Boolean
    ) : super(tree, data, loggableInsideSinglePage)

    constructor(
        tree: BTreeBase, data: ByteIterableWithAddress, size: Int,
        loggableInsideSinglePage: Boolean
    ) : super(tree, data, size, loggableInsideSinglePage)

    override val isBottom: Boolean
        get() = true

    override fun getChildAddress(index: Int): Long {
        return getKeyAddress(index)
    }

    override val bottomPagesCount: Long
        get() = 1

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

    override fun getMutableCopy(treeMutable: BTreeMutable): BasePageMutable {
        return BottomPageMutable(treeMutable, this)
    }

    override fun keyExists(key: ByteIterable): Boolean {
        return keyExists(key, this)
    }

    override fun exists(key: ByteIterable, value: ByteIterable): Boolean {
        return exists(key, value, this)
    }

    override fun childExists(key: ByteIterable, pageAddress: Long): Boolean {
        return false
    }

    override fun toString(): String {
        return "Bottom [" + size + "] @ " + (dataAddress - getIterable((size shl 1).toLong()).length - 1)
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        dump(out, level, renderer, this)
    }

    fun reclaim(keyIterable: ByteIterable, context: BTreeReclaimTraverser) {
        val node = context.currentNode
        if (node.isBottom) {
            if (node.dataAddress == dataAddress) {
                doReclaim(context)
                return
            } else if (node.size > 0 && node.minKey.compareKeyTo(keyIterable) == 0) {
                // we are already in desired bottom page, but address in not the same
                return
            }
        }
        // go up
        if (context.canMoveUp()) {
            while (true) {
                context.popAndMutate()
                context.moveRight()
                val index = context.getNextSibling(keyIterable)
                if (index < 0) {
                    if (!context.canMoveUp()) {
                        context.moveTo(max(-index - 2, 0))
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
            var index = context.getNextSibling(keyIterable)
            if (index < 0) {
                index = max(-index - 2, 0)
            }
            context.pushChild(index)
        }
        if (context.currentNode.dataAddress == dataAddress) {
            doReclaim(context)
        }
    }

    companion object {
        operator fun get(key: ByteIterable, page: BasePage): ILeafNode? {
            val index = page.binarySearch(key)
            return if (index >= 0) {
                page.getKey(index)
            } else null
        }

        private fun findFirst(stack: BTreeTraverser, depth: Int, page: BasePage): ILeafNode {
            val result: ILeafNode
            if (page.isBottom) {
                result = page.minKey
                stack.currentNode = page
                stack.currentPos = 0
                stack.top = depth
            } else {
                result = findFirst(stack, depth + 1, page.getChild(0))
                stack.setAt(depth, TreePos(page, 0))
            }
            return result
        }

        fun find(
            stack: BTreeTraverser,
            depth: Int,
            key: ByteIterable,
            value: ByteIterable?,
            equalOrNext: Boolean,
            page: BasePage
        ): ILeafNode? {
            var index = page.binarySearch(key)
            if (index < 0) {
                if (value == null && equalOrNext) {
                    index = -index - 1
                    if (index >= page.size) return null // after last element - no element to return
                } else {
                    return null
                }
            }
            val ln: ILeafNode = page.getKey(index)
            if (ln.isDup) {
                val dupRoot = ln.tree.root
                val dupLeaf: ILeafNode?
                if (value != null) {
                    // move dup cursor to requested value
                    dupLeaf = dupRoot.find(stack, depth + 1, value, null, equalOrNext)
                    if (dupLeaf == null) {
                        return null
                    }
                } else {
                    dupLeaf = findFirst(stack, depth + 1, dupRoot)
                }
                stack.setAt(depth, TreePos(page, index))
                (stack as BTreeTraverserDup).inDupTree = true
                return dupLeaf
            }
            if (stack.isDup) {
                (stack as BTreeTraverserDup).inDupTree = false
            }
            if (value == null || (if (equalOrNext) value <= ln.value else value.compareTo(ln.value) == 0)) {
                stack.currentNode = page
                stack.currentPos = index
                stack.top = depth
                return ln
            }
            return null
        }

        fun keyExists(key: ByteIterable, page: BasePage): Boolean {
            return page.binarySearch(key) >= 0
        }

        fun exists(key: ByteIterable, value: ByteIterable, page: BasePage): Boolean {
            val ln = page[key]
            return ln != null && ln.valueExists(value)
        }

        fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?, page: BasePage) {
            indent(out, level)
            out.println(page)
            for (i in 0 until page.size) {
                page.getKey(i).dump(out, level + 1, renderer)
            }
        }
    }
}
