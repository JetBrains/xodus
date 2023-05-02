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

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env.Cursor
import org.junit.Assert
import org.junit.Test

class BTreeCursorDeleteTest : BTreeTestBase() {
    @Test
    fun testDeleteCursorNoDuplicates1() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv(1, "1"))
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertTrue(c.next)
        Assert.assertTrue(c.deleteCurrent())
        Assert.assertFalse(c.deleteCurrent())
        Assert.assertFalse(c.next)
    }

    @Test
    fun testDeleteCursorNoDuplicates2() {
        treeMutable = createEmptyTreeForCursor(1).mutableCopy
        for (i in 0..7) {
            treeMutable!!.put(kv(i, "v$i"))
        }
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertTrue(c.next)
        Assert.assertEquals(key(0), c.key)
        Assert.assertTrue(c.deleteCurrent())
        Assert.assertFalse(c.deleteCurrent())
        Assert.assertTrue(c.next)
        Assert.assertEquals(key(1), c.key)
        Assert.assertEquals(value("v7"), c.getSearchKey(key(7)))
        Assert.assertTrue(c.deleteCurrent())
        Assert.assertFalse(c.deleteCurrent())
        Assert.assertFalse(c.next)
    }

    @Test
    fun testDeleteCursorDuplicates1() {
        treeMutable = createMutableTree(true, 1)
        treeMutable!!.put(kv(1, "11"))
        treeMutable!!.put(kv(1, "12"))
        Assert.assertTrue(treeMutable!!.root is BottomPageMutable)
        Assert.assertEquals(1, treeMutable!!.root.size.toLong())
        Assert.assertTrue(treeMutable!!.root.getKey(0) is LeafNodeDupMutable)
        Assert.assertEquals(2, treeMutable!!.root.getKey(0).dupCount)
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertTrue(c.next)
        Assert.assertTrue(c.deleteCurrent())
        Assert.assertFalse(c.deleteCurrent())
        Assert.assertTrue(treeMutable!!.root is BottomPageMutable)
        Assert.assertEquals(1, treeMutable!!.root.size.toLong())
        Assert.assertTrue(treeMutable!!.root.getKey(0) is LeafNodeMutable)
        Assert.assertTrue(c.next)
        Assert.assertTrue(c.deleteCurrent())
        Assert.assertFalse(c.deleteCurrent())
        Assert.assertFalse(c.next)
    }

    @Test
    fun testDeleteCursorDuplicates2() {
        treeMutable = createEmptyTreeForCursor(1).mutableCopy
        for (i in 0..7) {
            treeMutable!!.put(kv(i, "v$i"))
            treeMutable!!.put(kv(i, "vv$i"))
        }
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertTrue(c.getSearchBoth(key(1), value("vv1")))
        Assert.assertTrue(c.deleteCurrent())
        Assert.assertFalse(c.deleteCurrent())
        Assert.assertTrue(c.next)
        Assert.assertTrue(c.getSearchBoth(key(7), value("vv7")))
        Assert.assertTrue(c.deleteCurrent())
        Assert.assertFalse(c.next)
    }

    @Test
    fun testDeleteCursorDuplicates3() {
        treeMutable = createMutableTree(true, 1)!!.mutableCopy
        for (i in 0..31) {
            for (j in 0..31) {
                treeMutable!!.put(IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(j))
            }
        }
        for (i in 0..31) {
            val cursor: Cursor = treeMutable!!.openCursor()
            Assert.assertNotNull(cursor.getSearchKeyRange(IntegerBinding.intToEntry(i)))
            for (j in 0..30) {
                cursor.deleteCurrent()
                Assert.assertTrue(cursor.next)
            }
        }
    }
}
