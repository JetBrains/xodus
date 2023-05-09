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
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.tree.INode
import jetbrains.exodus.tree.btree.BTreeTraverser.Companion.getTraverserNoDup
import jetbrains.exodus.tree.btree.BTreeTraverser.Companion.isInDupMode
import org.junit.Assert
import org.junit.Test
import java.util.*

class BTreeReclaimTest : BTreeTestBase() {
    private fun init(p: Int): Long {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            1
        ).getMutableCopy()
        for (i in 0 until p) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        return saveTree()
    }

    private fun initDup(p: Int, u: Int): Long {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            true,
            1
        ).getMutableCopy()
        for (i in 0 until p) {
            for (j in 0 until u) {
                treeMutable!!.put(kv(i, "v$i#$j"))
            }
        }
        return saveTree()
    }

    @Test
    fun testLeafSimple() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            1
        ).getMutableCopy()
        treeMutable!!.put(kv(0, "nothing"))
        val rootAddress = saveTree()
        tree = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1)
        val key: ByteIterable = key(0)
        val savedLeaf = tree!!.getRoot()[key]
        Assert.assertNotNull(savedLeaf)
        val savedLeafAddress = savedLeaf!!.getAddress()
        treeMutable = tree!!.getMutableCopy()
        treeMutable!!.put(kv(0, "anything"))
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy()
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(savedLeafAddress)
        Assert.assertTrue(treeMutable!!.reclaim(iter.next(), iter))
        val addressIterator = getTreeAddresses(tree!!)
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            isAffected(log!!.read(address), key, addressIterator.traverser as BTreeTraverser)
        }
    }

    @Test
    fun testLeafSimpleRemove() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            1
        ).getMutableCopy()
        treeMutable!!.put(kv(0, "thing"))
        treeMutable!!.put(kv(1, "nothing"))
        treeMutable!!.put(kv(2, "something"))
        treeMutable!!.put(kv(3, "jumping"))
        treeMutable!!.put(kv(4, "dumping"))
        treeMutable!!.put(kv(5, "rambling"))
        treeMutable!!.put(kv(6, "plumbing"))
        var rootAddress = saveTree()
        tree = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1)
        val key: ByteIterable = key(0)
        val savedLeaf = tree!!.getRoot()[key]
        Assert.assertNotNull(savedLeaf)
        val savedLeafAddress = savedLeaf!!.getAddress()
        treeMutable = tree!!.getMutableCopy()
        treeMutable!!.delete(key(1))
        rootAddress = saveTree()
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy()
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(savedLeafAddress)
        Assert.assertTrue(treeMutable!!.reclaim(iter.next(), iter))
        println(treeMutable!!.getExpiredLoggables().size())
        val addressIterator = getTreeAddresses(tree!!)
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            isAffected(log!!.read(address), key, addressIterator.traverser as BTreeTraverser)
        }
    }

    @Test
    fun testRootSimple() {
        treeMutable = BTreeEmpty(
            log!!,
            createTestSplittingPolicy(),
            false,
            1
        ).getMutableCopy()
        treeMutable!!.put(kv(0, "nothing"))
        treeMutable!!.put(kv(1, "something"))
        var rootAddress = saveTree()
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy()
        treeMutable!!.delete(key(0))
        treeMutable!!.delete(key(1))
        rootAddress = saveTree()
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy()
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(rootAddress)
        Assert.assertTrue(treeMutable!!.reclaim(iter.next(), iter)) // root should be reclaimed
    }

    @Test
    fun testLeafNoDup() {
        var p: Int
        var rootAddress = init(1000.also { p = it })
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy()
        val key: ByteIterable = key(0)
        val savedLeaf = tree!!.getRoot()[key]
        Assert.assertNotNull(savedLeaf)
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(
            savedLeaf!!.getAddress()
        )
        Assert.assertTrue(treeMutable!!.reclaim(iter.next(), iter))
        val addressIterator = getTreeAddresses(tree!!)
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            isAffected(log!!.read(address), key, addressIterator.traverser as BTreeTraverser)
        }
        rootAddress = saveTree()
        checkTree(BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy().also { treeMutable = it }, p).run()
    }

    @Test
    fun testPageNoDup() {
        var p: Int
        var rootAddress = init(1000.also { p = it })
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy()
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(0)
        val leaf = iter.next()
        var next: RandomAccessLoggable
        while (true) {
            next = iter.next()
            if (next.getType() == BTreeBase.INTERNAL) {
                break
            }
        }
        val page: BasePage = tree!!.loadPage(next.getAddress())
        Assert.assertTrue(treeMutable!!.reclaim(leaf, iter))
        val addressIterator = getTreeAddresses(tree!!)
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            assertAffected(
                log!!.read(address),
                page,
                addressIterator.traverser as BTreeTraverser
            )
        }
        rootAddress = saveTree()
        checkTree(BTree(log!!, treeMutable!!.balancePolicy, rootAddress, false, 1).also {
            tree = it
        }.getMutableCopy().also { treeMutable = it }, p).run()
    }

    @Test
    fun testDupLeaf() {
        var p: Int
        var u: Int
        var rootAddress = initDup(10.also { p = it }, 100.also { u = it })
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy()
        val key: ByteIterable = key(0)
        val savedLeaf = tree!!.getRoot()[key]
        Assert.assertNotNull(savedLeaf)
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(
            savedLeaf!!.getAddress()
        )
        Assert.assertTrue(treeMutable!!.reclaim(iter.next(), iter))
        val addressIterator = getTreeAddresses(tree!!)
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            isAffected(log!!.read(address), key, addressIterator.traverser as BTreeTraverser)
            val loggable: Loggable = log!!.read(address)
            if (isInDupMode(addressIterator) && isAffected(loggable, key, getTraverserNoDup(addressIterator))) {
                assertAffected(treeMutable!!, tree!!, loggable, key)
            }
        }
        rootAddress = saveTree()
        checkTree(BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy().also { treeMutable = it }, p, u).run()
    }

    @Test
    fun testSkipDupTree() {
        /* int p;
        int u; */
        var rootAddress = initDup( /* p = */10,  /* u = */100)
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy()
        val key: ByteIterable = key(5)
        val savedLeaf = tree!!.getRoot()[key]
        Assert.assertNotNull(savedLeaf)
        val oldAddress = savedLeaf!!.getAddress()
        treeMutable!!.delete(key)
        treeMutable!!.delete(key(6))
        treeMutable!!.put(key(6), value("v6#0"))
        rootAddress = saveTree()
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy()
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(oldAddress)
        Assert.assertTrue(treeMutable!!.reclaim(iter.next(), iter))
        val addressIterator = getTreeAddresses(tree!!)
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            isAffected(log!!.read(address), key, addressIterator.traverser as BTreeTraverser)
            val loggable: RandomAccessLoggable = log!!.read(address)
            if (isInDupMode(addressIterator) && isAffected(loggable, key, getTraverserNoDup(addressIterator))) {
                assertAffected(treeMutable!!, tree!!, loggable, key)
            }
        }
    }

    @Test
    fun testLeafDup() {
        var p: Int
        var u: Int
        val rootAddress = initDup(10.also { p = it }, 100.also { u = it })
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy()
        val key: ByteIterable = key(0)
        val value: ByteIterable = value("v0#10")
        val dupLeaf = tree!!.getRoot()[key] as LeafNodeDup?
        Assert.assertNotNull(dupLeaf)
        val dt = dupLeaf!!.getTree()
        val savedLeaf = dt.getRoot()[value]
        Assert.assertNotNull(savedLeaf)
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(
            savedLeaf!!.getAddress()
        )
        Assert.assertTrue(treeMutable!!.reclaim(iter.next(), iter))
        val addressIterator = getTreeAddresses(tree!!)
        val dupLeafMutable = treeMutable!!.getRoot()[key] as LeafNodeDupMutable?
        Assert.assertNotNull(dupLeafMutable)
        val dtm = dupLeafMutable!!.tree
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            val loggable: Loggable = log!!.read(address)
            if (isInDupMode(addressIterator) && isAffected(loggable, key, getTraverserNoDup(addressIterator))) {
                assertAffected(dtm, dt, loggable, value, addressIterator.traverser as BTreeTraverser)
            }
        }
        checkTree(BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy().also { treeMutable = it }, p, u).run()
    }

    @Test
    fun testPageDup() {
        var p: Int
        var u: Int
        val rootAddress = initDup(10.also { p = it }, 100.also { u = it })
        treeMutable = BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy()
        val key: ByteIterable = key(0)
        val value: ByteIterable = value("v0#10")
        val dupLeaf = tree!!.getRoot()[key] as LeafNodeDup?
        Assert.assertNotNull(dupLeaf)
        val dt: BTreeBase = dupLeaf!!.getTree()
        val savedLeaf = dt.getRoot()[value]
        Assert.assertNotNull(savedLeaf)
        val iter: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(
            savedLeaf!!.getAddress()
        )
        var loggable = iter.next()
        while (loggable.getType() != BTreeBase.DUP_INTERNAL) {
            loggable = iter.next()
        }
        val page: BasePage = dt.loadPage(loggable.getAddress())
        Assert.assertTrue(treeMutable!!.reclaim(loggable, iter))
        val addressIterator = getTreeAddresses(tree!!)
        val dupLeafMutable = treeMutable!!.getRoot()[key] as LeafNodeDupMutable?
        Assert.assertNotNull(dupLeafMutable)
        val dtm: BTreeMutable = dupLeafMutable!!.tree
        while (addressIterator.hasNext()) {
            val address = addressIterator.next()
            loggable = log!!.read(address)
            if (isInDupMode(addressIterator) && isAffected(loggable, value, getTraverserNoDup(addressIterator))) {
                assertAffected(dtm, dt, loggable, page, addressIterator.traverser as BTreeTraverser)
            }
        }
        checkTree(BTree(log!!, treeMutable!!.balancePolicy, rootAddress, true, 1).also {
            tree = it
        }.getMutableCopy().also { treeMutable = it }, p, u).run()
    }

    private fun isAffected(loggable: Loggable, key: ByteIterable, path: BTreeTraverser): Boolean {
        return when (loggable.getType()) {
            BTreeBase.BOTTOM_ROOT, BTreeBase.INTERNAL_ROOT -> {
                Assert.assertEquals(tree!!.getRootAddress(), loggable.getAddress())
                assertAffected(treeMutable!!, tree!!.getRoot(), key, path)
                true
            }

            BTreeBase.BOTTOM, BTreeBase.INTERNAL -> {
                assertAffected(treeMutable!!, tree!!.loadPage(loggable.getAddress()), key, path)
                true
            }

            BTreeBase.LEAF, BTreeBase.LEAF_DUP_BOTTOM_ROOT, BTreeBase.LEAF_DUP_INTERNAL_ROOT -> assertAffected(
                treeMutable!!,
                tree!!.loadLeaf(loggable.getAddress()),
                key
            )

            else -> false
        }
    }

    private fun assertAffected(loggable: Loggable, page: BasePage, path: BTreeTraverser) {
        when (loggable.getType()) {
            BTreeBase.BOTTOM_ROOT, BTreeBase.INTERNAL_ROOT -> {
                Assert.assertEquals(tree!!.getRootAddress(), loggable.getAddress())
                assertAffected(treeMutable!!, tree!!.getRoot(), page, path)
                return
            }

            BTreeBase.BOTTOM, BTreeBase.INTERNAL -> {
                assertAffected(treeMutable!!, tree!!.loadPage(loggable.getAddress()), page, path)
                return
            }

            else -> {}
        }
    }

    companion object {
        private fun assertAffected(
            dtm: BTreeMutable, dt: BTreeBase, loggable: Loggable,
            value: ByteIterable, path: BTreeTraverser
        ) {
            when (loggable.getType()) {
                BTreeBase.LEAF_DUP_BOTTOM_ROOT, BTreeBase.LEAF_DUP_INTERNAL_ROOT -> {
                    assertAffected(dtm, dt.getRoot(), value, path)
                    return
                }

                BTreeBase.DUP_BOTTOM, BTreeBase.DUP_INTERNAL -> {
                    assertAffected(dtm, dt.loadPage(loggable.getAddress()), value, path)
                    return
                }

                BTreeBase.DUP_LEAF -> {
                    assertAffected(dtm, dt.loadLeaf(loggable.getAddress()), value)
                    return
                }

                else -> throw IllegalStateException("loggable type unexpected in duplicates tree")
            }
        }

        private fun assertAffected(
            dtm: BTreeMutable, dt: BTreeBase,
            loggable: Loggable, key: ByteIterable
        ) {
            when (loggable.getType()) {
                BTreeBase.LEAF_DUP_BOTTOM_ROOT, BTreeBase.LEAF_DUP_INTERNAL_ROOT -> {
                    assertAffected(dtm, dt.loadLeaf(loggable.getAddress()), key)
                    return
                }

                BTreeBase.DUP_BOTTOM, BTreeBase.DUP_INTERNAL, BTreeBase.DUP_LEAF -> return
                else -> throw IllegalStateException("loggable type unexpected in duplicates tree")
            }
        }

        private fun assertAffected(
            dtm: BTreeMutable, dt: BTreeBase, loggable: RandomAccessLoggable,
            page: BasePage, path: BTreeTraverser
        ) {
            when (loggable.getType()) {
                BTreeBase.LEAF_DUP_BOTTOM_ROOT, BTreeBase.LEAF_DUP_INTERNAL_ROOT -> {
                    assertAffected(dtm, dt.getRoot(), page, path)
                    return
                }

                BTreeBase.DUP_BOTTOM, BTreeBase.DUP_INTERNAL -> {
                    assertAffected(dtm, dt.loadPage(loggable.getAddress()), page, path)
                    return
                }

                else -> {}
            }
        }

        private fun assertAffected(treeMutable: BTreeMutable, leaf: INode, key: ByteIterable): Boolean {
            if (isEqual(leaf.getKey(), key)) {
                val ln = treeMutable.getRoot()[key]
                Assert.assertNotNull(ln)
                Assert.assertTrue(ln!!.isMutable())
                return true
            }
            return false
        }

        private fun assertAffected(treeMutable: BTreeMutable, page: BasePage, key: ByteIterable, path: BTreeTraverser) {
            if (isBetween(page.getMinKey().getKey(), key, page.getMaxKey().getKey())) {
                Assert.assertTrue(getPage(treeMutable, path).isMutable())
            }
        }

        private fun assertAffected(
            treeMutable: BTreeMutable,
            sourcePage: BasePage,
            page: BasePage,
            path: BTreeTraverser
        ) {
            if (isBetween(
                    sourcePage.getMinKey().getKey(),
                    page.getMinKey().getKey(),
                    sourcePage.getMaxKey().getKey()
                )
            ) {
                Assert.assertTrue(getPage(treeMutable, path).isMutable())
            }
        }

        private fun isBetween(min: ByteIterable, current: ByteIterable, max: ByteIterable): Boolean {
            return Arrays.compareUnsigned(
                min.bytesUnsafe, 0, min.length,
                current.bytesUnsafe, 0, current.length
            ) <= 0 && Arrays.compareUnsigned(
                current.bytesUnsafe, 0, current.length,
                max.bytesUnsafe, 0, max.length
            ) <= 0
        }

        private fun isEqual(found: ByteIterable, current: ByteIterable): Boolean {
            return Arrays.compareUnsigned(
                found.bytesUnsafe, 0, found.length,
                current.bytesUnsafe, 0, current.length
            ) == 0
        }

        private fun getPage(treeMutable: BTreeMutable, path: BTreeTraverser): BasePage {
            var result: BasePage = treeMutable.getRoot()
            val itr = path.iterator()
            while (itr.hasNext()) {
                val node = itr.next()
                if (result.isBottom() xor node!!.isBottom()) {
                    Assert.fail("Tree structure not matched by type")
                }
                if (itr.hasNext()) {
                    if (result.isBottom()) {
                        Assert.fail("Tree structure not matched by depth")
                    } else {
                        result = result.getChild((node.size - 1).coerceAtMost(itr.getPos()))
                    }
                }
            }
            return result
        }

        private fun getTreeAddresses(tree: BTreeBase): AddressIterator {
            return tree.addressIterator()
        }
    }
}
