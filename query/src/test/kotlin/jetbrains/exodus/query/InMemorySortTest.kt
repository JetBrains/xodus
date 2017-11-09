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

import jetbrains.exodus.entitystore.ComparableGetter
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityStoreTestBase
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test
import java.util.*

private const val PAGE_SIZE = 100

class InMemorySortTest : EntityStoreTestBase() {

    private var sum: Int = 0

    private val comparator = compareBy<Entity> { it.getProperty("int") }
    private val valueGetter = ComparableGetter { it.getProperty("int") }
    private val valueComparator = Comparator<Comparable<Any>> { o1, o2 -> o1.compareTo(o2) }

    override fun setUp() {
        super.setUp()
        val rnd = Random()
        sum = 0
        repeat(PAGE_SIZE, {
            storeTransaction.newEntity("Issue").setProperty("int", it)
            sum += it
        })
        repeat(15000, {
            val value = Math.abs(rnd.nextInt(Int.MAX_VALUE - PAGE_SIZE) + PAGE_SIZE)
            storeTransaction.newEntity("Issue").setProperty("int", value)
        })
        storeTransaction.flush()
    }

    @Test
    fun testMergeSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryMergeSortIterable(it, comparator) })
    }

    @Test
    fun testMergeSortWithValueGetter() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryMergeSortIterableWithValueGetter(it, valueGetter, valueComparator) }, valueGetter, valueComparator)
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
    fun testHeapSortWithValueGetter() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryHeapSortIterableWithValueGetter(it, valueGetter, valueComparator) }, valueGetter, valueComparator)
    }

    @Test
    fun testKeapSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryKeapSortIterable(it, comparator) })
    }

    @Test
    fun testBoundedSort() {
        testSort(storeTransaction.getAll("Issue"),
                { InMemoryBoundedHeapSortIterable(PAGE_SIZE, it, comparator) })
    }

    private fun testSort(it: Iterable<Entity>, sortFun: (it: Iterable<Entity>) -> SortEngine.InMemorySortIterable) {
        val sorted = sortFun(it)
        var prev: Entity? = null
        var sum = 0
        sorted.take(PAGE_SIZE).forEach {
            prev?.apply {
                Assert.assertTrue(sorted.comparator.compare(this, it) <= 0)
            }
            sum += it.getProperty("int") as Int
            prev = it
        }
        Assert.assertEquals(this.sum, sum)
    }

    private fun testSort(it: Iterable<Entity>, sortFun: (it: Iterable<Entity>) -> Iterable<Entity>, valueGetter: ComparableGetter, comparator: Comparator<Comparable<Any>>) {
        val sorted = sortFun(it)
        var prev: Entity? = null
        var sum = 0
        sorted.take(PAGE_SIZE).forEach {
            prev?.apply {
                Assert.assertTrue(comparator.compare(valueGetter.select(this), valueGetter.select(it)) <= 0)
            }
            sum += it.getProperty("int") as Int
            prev = it
        }
        Assert.assertEquals(this.sum, sum)
    }
}
