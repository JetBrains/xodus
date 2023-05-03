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
import org.junit.Assert
import org.junit.Test

class BTreeSpecificCursorDuplicatesTest : BTreeTestBase() {
    @Test
    fun testGetSearchKeyRange3() {
        treeMutable = createEmptyTreeForCursor(1).mutableCopy
        treeMutable!!.put(kv(2, "v1"))
        treeMutable!!.put(kv(2, "v2"))
        treeMutable!!.put(kv(2, "v3"))
        treeMutable!!.put(kv(3, "v5"))
        treeMutable!!.put(kv(3, "v6"))
        treeMutable!!.put(kv(3, "v7"))
        assertMatches(treeMutable!!, bp(2))
        val getDups: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                c.getSearchKey(key(2))
                Assert.assertTrue(c.nextDup)
                Assert.assertTrue(c.nextDup)
                Assert.assertFalse(c.nextDup)
            }
        }
        getDups.run()
        val a = saveTree()
        getDups.run()
        reopen()
        getDups.setTree(openTree(a, true))
        getDups.run()
    }
}
