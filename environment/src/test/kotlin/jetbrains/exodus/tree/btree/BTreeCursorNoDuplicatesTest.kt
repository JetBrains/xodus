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

import jetbrains.exodus.env.Cursor
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.ITreeMutable
import jetbrains.exodus.tree.TreeCursorNoDuplicatesTest
import org.junit.Assert
import org.junit.Test

/**
 *
 */
class BTreeCursorNoDuplicatesTest : TreeCursorNoDuplicatesTest() {
    override fun createMutableTree(hasDuplicates: Boolean, structureId: Int): ITreeMutable {
        return BTreeEmpty(log!!, false, structureId).getMutableCopy()
    }

    override fun openTree(address: Long, hasDuplicates: Boolean): ITree {
        return BTree(log!!, address, hasDuplicates, 1)
    }

    @Test
    fun testGetNextDup() {
        val genNextDup: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                for (i in 0..999) {
                    Assert.assertTrue(c.next)
                    Assert.assertFalse(c.nextDup)
                }
            }
        }
        genNextDup.run()
        val a = saveTree()
        genNextDup.run()
        reopen()
        genNextDup.setTree(openTree(a, true))
        genNextDup.run()
    }
}
