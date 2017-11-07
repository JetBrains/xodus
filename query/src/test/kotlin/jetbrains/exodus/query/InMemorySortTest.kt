/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityStoreTestBase
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test

class InMemorySortTest : EntityStoreTestBase() {

    var sum: Int = 0

    val comparator = compareBy<Entity> { it.getProperty("int") }

    override fun setUp() {
        super.setUp()
        val rnd = Random()
        sum = 0
        repeat(15000, {
            val value = Math.abs(rnd.nextInt())
            storeTransaction.newEntity("Issue").setProperty("int", value)
            sum += value
        })
        storeTransaction.flush()
    }

    @Test
    fun testMergeSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryMergeSortIterable(it, comparator) })
    }

    @Test
    fun testMergeSortWithArrayList() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryMergeSortIterableWithArrayList(it, comparator) })
    }

    @Test
    fun testTimSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryTimSortIterable(it, comparator) })
    }

    @Test
    fun testQuickSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryQuickSortIterable(it, comparator) })
    }

    @Test
    fun testHeapSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryHeapSortIterable(it, comparator) })
    }

    @Test
    fun testKeapSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryKeapSortIterable(it, comparator) })
    }

    @Test
    fun testBoundedSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryBoundedHeapSortIterable(15000, it, comparator) })
    }

    private fun testSort(it: Iterable<Entity>, sortFun: (it: Iterable<Entity>) -> SortEngine.InMemorySortIterable) {
        val sorted = sortFun(it)
        var prev: Entity? = null
        var sum = 0
        sorted.forEach {
            prev?.apply {
                Assert.assertTrue(sorted.comparator.compare(this, it) <= 0)
            }
            sum += it.getProperty("int") as Int
            prev = it
        }
        Assert.assertEquals(this.sum, sum)
    }
}
