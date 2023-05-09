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

import jetbrains.exodus.tree.TreeBaseTest
import org.junit.Assert

open class BTreeTestBase : TreeBaseTest<BTreeBase, BTreeMutable>() {
    override var tree: BTreeBase?
        get() = super.tree as BTreeBase
        set(tree) {
            super.tree = tree
        }
    override var treeMutable: BTreeMutable?
        get() = super.treeMutable as BTreeMutable
        set(treeMutable) {
            super.treeMutable = treeMutable
        }

    fun assertMatches(t: BTreeBase, ip: IP) {
        ip.matches(t.getRoot())
    }

    fun ip(vararg children: IP): IP {
        return IP(*children)
    }

    fun bp(size: Int): BP {
        return BP(size)
    }

    protected fun createEmptyTreeForCursor(@Suppress("SameParameterValue") structureId: Int): BTreeEmpty {
        return BTreeEmpty(log!!, BTreeBalancePolicy(4), true, structureId)
    }

    override fun createMutableTree(hasDuplicates: Boolean, structureId: Int): BTreeMutable? {
        return doCreateMutableTree(hasDuplicates, structureId)
    }

    override fun openTree(address: Long, hasDuplicates: Boolean): BTree? {
        return doOpenTree(address, hasDuplicates)
    }

    open class IP internal constructor(vararg children: IP) {
        private var children: Array<out IP>

        init {
            this.children = children
        }

        open fun matches(p: BasePage) {
            Assert.assertTrue(p is InternalPage || p is InternalPageMutable)
            Assert.assertEquals(children.size.toLong(), p.size.toLong())
            for (i in 0 until p.size) {
                children[i].matches(p.getChild(i))

                // check min key == first key of children
                Assert.assertEquals(p.getChild(i).getKey(0), p.getKey(i))
            }
        }
    }

    class BP internal constructor(var size: Int) : IP() {
        override fun matches(p: BasePage) {
            Assert.assertTrue(p is BottomPage || p is BottomPageMutable)
            Assert.assertEquals(size.toLong(), p.size.toLong())
        }
    }

    companion object {
        fun createTestSplittingPolicy(): BTreeBalancePolicy {
            return BTreeBalancePolicy(5)
        }

        fun doCreateMutableTree(hasDuplicates: Boolean, structureId: Int): BTreeMutable {
            return BTreeEmpty(
                log!!,
                createTestSplittingPolicy(),
                hasDuplicates,
                structureId
            ).getMutableCopy()
        }

        fun doOpenTree(address: Long, hasDuplicates: Boolean): BTree {
            return BTree(log!!, createTestSplittingPolicy(), address, hasDuplicates, 1)
        }
    }
}
