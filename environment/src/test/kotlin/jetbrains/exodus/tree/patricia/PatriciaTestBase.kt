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
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.ITreeMutable
import jetbrains.exodus.tree.TreeBaseTest
import org.junit.Assert

open class PatriciaTestBase : TreeBaseTest<ITree, ITreeMutable>() {
    override var tree: ITree?
        get() = super.tree
        set(tree) {
            super.tree = tree
        }
    override var treeMutable: ITreeMutable?
        get() = super.treeMutable
        set(treeMutable) {
            super.treeMutable = treeMutable
        }

    override fun createMutableTree(hasDuplicates: Boolean, structureId: Int): ITreeMutable? {
        return doCreateMutableTree(hasDuplicates, structureId)
    }

    override fun openTree(address: Long, hasDuplicates: Boolean): ITree? {
        return doOpenTree(address, hasDuplicates)
    }

    fun assertMatches(t: ITree, node: N) {
        val tree = t as PatriciaTreeBase
        node.matches(tree, tree.root)
    }

    class N internal constructor(
        private var mutable: Boolean,
        var c: Char,
        expectedKey: ByteIterable,
        expectedValue: ByteIterable?,
        vararg expectedChildren: N
    ) {
        private var children: Array<out N>
        private var keySequence: ByteIterable
        var value: ByteIterable?

        internal constructor(isMutable: Boolean, vararg expectedChildren: N) : this(isMutable, '*', *expectedChildren)

        internal constructor(
            isMutable: Boolean,
            expectedKey: ByteIterable,
            expectedValue: ByteIterable?,
            vararg expectedChildren: N
        ) : this(isMutable, '*', expectedKey, expectedValue, *expectedChildren)

        internal constructor(isMutable: Boolean, expectedChar: Char, vararg expectedChildren: N) : this(
            isMutable,
            expectedChar,
            ByteIterable.EMPTY,
            *expectedChildren
        )

        internal constructor(
            isMutable: Boolean,
            expectedChar: Char,
            expectedKey: ByteIterable,
            vararg expectedChildren: N
        ) : this(isMutable, expectedChar, expectedKey, null, *expectedChildren)

        init {
            children = expectedChildren
            keySequence = expectedKey
            value = expectedValue
        }

        fun matches(tree: PatriciaTreeBase?, node: NodeBase?) {
            Assert.assertEquals(mutable, node!!.isMutable)
            Assert.assertEquals(children.size, node.childrenCount)
            assertIterablesMatch(keySequence, node.key)
            assertIterablesMatch(value, node.value)
            for ((i, ref) in node.children.withIndex()) {
                val expectedChild = children[i]
                Assert.assertEquals(expectedChild.c.code, ref.firstByte.toInt())
                val child = ref.getNode(tree!!)
                expectedChild.matches(tree, child)
            }
        }
    }

    companion object {
        fun doCreateMutableTree(hasDuplicates: Boolean, structureId: Int): ITreeMutable {
            return PatriciaTreeEmpty(log!!, structureId, hasDuplicates).mutableCopy
        }

        fun doOpenTree(address: Long, hasDuplicates: Boolean): ITree {
            val tree = PatriciaTree(log!!, address, 1)
            return if (hasDuplicates) PatriciaTreeWithDuplicates(tree) else tree
        }

        fun r(vararg expectedChildren: N): N {
            return N(false, *expectedChildren)
        }

        fun r(key: String, vararg expectedChildren: N): N {
            return r(key, null, *expectedChildren)
        }

        fun r(key: String, value: String?, vararg expectedChildren: N): N {
            return N(
                false, ArrayByteIterable(key.toByteArray()),
                if (value == null) null else ArrayByteIterable(value.toByteArray()), *expectedChildren
            )
        }

        fun n(c: Char, key: String, value: String?, vararg expectedChildren: N): N {
            return N(
                false, c, ArrayByteIterable(key.toByteArray()),
                if (value == null) null else ArrayByteIterable(value.toByteArray()), *expectedChildren
            )
        }

        fun n(c: Char, vararg expectedChildren: N): N {
            return n(c, "", null as String?, *expectedChildren)
        }

        fun n(c: Char, key: String, vararg expectedChildren: N): N {
            return n(c, key, null, *expectedChildren)
        }

        fun rm(key: String, vararg expectedChildren: N): N {
            return rm(key, value = null, *expectedChildren)
        }

        private fun rm(
            key: String, @Suppress("SameParameterValue") value: String?,
            vararg expectedChildren: N
        ): N {
            return N(
                true, ArrayByteIterable(key.toByteArray()),
                if (value == null) {
                    null
                } else {
                    ArrayByteIterable(value.toByteArray())
                }, *expectedChildren
            )
        }

        fun nm(c: Char, key: String, value: String?, vararg expectedChildren: N): N {
            return N(
                true, c, ArrayByteIterable(key.toByteArray()),
                if (value == null) null else ArrayByteIterable(value.toByteArray()), *expectedChildren
            )
        }

        fun nm(c: Char, vararg expectedChildren: N): N {
            return nm(c, "", null as String?, *expectedChildren)
        }

        fun nm(c: Char, key: String, vararg expectedChildren: N): N {
            return nm(c, key, value = null, *expectedChildren)
        }
    }
}
