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

import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.*
import java.io.PrintStream

internal class TreeAwareNodeDecorator(val tree: PatriciaTreeBase, private val decorated: NodeBase) : NodeBase(
    decorated.key, decorated.value
) {
    override val address: Long
        get() = decorated.address
    override val isMutable: Boolean
        get() = decorated.isMutable

    override fun getMutableCopy(mutableTree: PatriciaTreeMutable): MutableNode {
        return decorated.getMutableCopy(mutableTree)
    }

    override fun getChild(tree: PatriciaTreeBase, b: Byte): NodeBase? {
        return decorated.getChild(tree, b)
    }

    override fun getChildren(b: Byte): NodeChildrenIterator {
        return decorated.getChildren(b)
    }

    override fun getChildrenRange(b: Byte): NodeChildrenIterator {
        return decorated.getChildrenRange(b)
    }

    override val childrenLast: NodeChildrenIterator
        get() = decorated.childrenLast
    override val children: NodeChildren
        get() = decorated.children
    override val childrenCount: Int
        get() = decorated.childrenCount

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        dump(tree, this, out, level, renderer)
    }

    companion object {
        private fun dump(
            tree: PatriciaTreeBase,
            node: NodeBase,
            out: PrintStream,
            level: Int,
            renderer: Dumpable.ToString?
        ) {
            out.println(
                String.format(
                    "node %s%s] %s",
                    if (node.isMutable) "(m) [" else '[',
                    node.childrenCount,
                    if (renderer == null) node.toString() else renderer.toString(node)
                )
            )
            for (child in node.children) {
                indent(out, level)
                val childAddress = child.suffixAddress
                out.println(child.firstByte.toString() + " -> " + childAddress)
                indent(out, level + 1)
                out.print("+ ")
                if (childAddress == Loggable.NULL_ADDRESS || tree.log.hasAddress(childAddress)) {
                    dump(tree, child.getNode(tree), out, level + 3, renderer)
                } else {
                    out.println("[not found]")
                }
            }
        }
    }
}
