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
import jetbrains.exodus.env.Cursor
import org.junit.Assert
import org.junit.Before
import org.junit.Test

/**
 *
 */
open class BTreeCursorDupConcurrentModificationTest : BTreeTestBase() {
    @Before
    fun prepareTree() {
        treeMutable = BTreeEmpty(log!!, BTreeBalancePolicy(5), true, 1).mutableCopy
        treeMutable!!.put(key(1), value("v10"))
        treeMutable!!.put(key(2), value("v20"))
        treeMutable!!.put(key(3), value("v30"))
        treeMutable!!.put(key(4), value("v40"))
        treeMutable!!.put(key(5), value("v50"))
        treeMutable!!.put(key(5), value("v51"))
        treeMutable!!.put(key(5), value("v52"))
        treeMutable!!.put(key(6), value("v60"))
        treeMutable!!.put(key(7), value("v70"))
    }

    @Test
    fun testConcurrentDeleteBefore1() {
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertTrue(c.getSearchBoth(key(5), value("v51")))
        deleteImpl(key(1))
        Assert.assertTrue(c.next)
        Assert.assertEquals(key(5), c.key)
        Assert.assertEquals(value("v52"), c.value)
        Assert.assertTrue(c.next)
        Assert.assertEquals(key(6), c.key)
    }

    @Test
    fun testConcurrentDeleteAfter() {
        val c: Cursor = treeMutable!!.openCursor()
        Assert.assertTrue(c.getSearchBoth(key(5), value("v51")))
        deleteImpl(key(6))
        Assert.assertTrue(c.next)
        Assert.assertEquals(key(5), c.key)
        Assert.assertEquals(value("v52"), c.value)
        Assert.assertTrue(c.next)
        Assert.assertEquals(key(7), c.key)
    }

    @Test
    fun testConcurrentDeleteCurrent() {
        val c: Cursor = treeMutable!!.openCursor()
        val value: ByteIterable = value("v51")
        Assert.assertTrue(c.getSearchBoth(key(5), value))
        deleteImpl(key(5), value)
        Assert.assertTrue(c.next)
        Assert.assertEquals(key(5), c.key)
        Assert.assertEquals(value("v52"), c.value)
    }

    protected open fun deleteImpl(key: ByteIterable) {
        Assert.assertTrue(treeMutable!!.delete(key))
    }

    protected open fun deleteImpl(key: ByteIterable, value: ByteIterable) {
        Assert.assertTrue(treeMutable!!.delete(key, value))
    }
}
