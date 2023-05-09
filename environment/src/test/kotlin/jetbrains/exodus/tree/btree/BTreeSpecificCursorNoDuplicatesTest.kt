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

class BTreeSpecificCursorNoDuplicatesTest : BTreeTestBase() {
    @Test
    fun testGetSearchKeyRange3() {
        treeMutable = createEmptyTreeForCursor(1).getMutableCopy()
        treeMutable!!.put(kv(1, "v1"))
        treeMutable!!.put(kv(2, "v2"))
        treeMutable!!.put(kv(3, "v3"))
        treeMutable!!.put(kv(5, "v5"))
        treeMutable!!.put(kv(6, "v6"))
        treeMutable!!.put(kv(7, "v7"))
        assertMatches(treeMutable!!, ip(bp(3), bp(3)))
        val getSearchKeyRange: TreeAwareRunnable = object : TreeAwareRunnable(treeMutable) {
            override fun run() {
                val c: Cursor = t!!.openCursor()
                Assert.assertEquals(
                    value("v5"),
                    c.getSearchKeyRange(key(4))
                )
                Assert.assertEquals(key(5), c.key)
            }
        }
        getSearchKeyRange.run()
        val a = saveTree()
        getSearchKeyRange.run()
        reopen()
        getSearchKeyRange.setTree(openTree(a, true))
        getSearchKeyRange.run()
    }
}
