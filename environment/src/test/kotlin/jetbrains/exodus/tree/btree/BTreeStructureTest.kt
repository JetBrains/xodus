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
import org.junit.Assert
import org.junit.Test

class BTreeStructureTest : BTreeTestBase() {
    private var policy = BTreeBalancePolicy(10)
    fun add(a: String): BTreeMutable {
        treeMutable!!.add(key(a), value("v " + `val`++))
        return treeMutable!!
    }

    operator fun get(a: String): ByteIterable? {
        return treeMutable!![key(a)]
    }

    private fun refresh(): BTreeMutable {
        val a = saveTree()
        tree = BTree(log!!, policy, a, false, 1)
        treeMutable = tree!!.mutableCopy
        return treeMutable!!
    }

    @Test
    fun simple() {
        treeMutable = BTreeEmpty(log!!, policy, false, 1).mutableCopy
        add("c")
        for (i in 0..6) {
            add("a$i")
        }
        refresh()
        for (i in 0..2) {
            add("e$i")
        }
        add("d9")
        add("d8")
        Assert.assertNotNull(get("c"))
    }

    @Test
    fun childExistsTest() {
        treeMutable = BTreeEmpty(log!!, policy, false, 1).mutableCopy
        for (i in 0..60) {
            treeMutable!!.add(key("k $i"), value("v $i"))
        }
        refresh()
        val root: BasePage = treeMutable!!.root
        val child = root.getChild(0) as InternalPage
        Assert.assertTrue(root.childExists(child.getKey(0).key, child.getChildAddress(0)))
    }

    companion object {
        var `val` = 0
    }
}
