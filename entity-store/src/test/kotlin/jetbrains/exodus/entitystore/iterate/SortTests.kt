/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.TestFor
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.EntityStoreTestBase
import org.junit.Assert

class SortTests : EntityStoreTestBase() {

    fun testSort() {
        val txn = storeTransaction
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", 100 - i)
        }
        for (i in 0..99) {
            txn.newEntity("Issue")
        }
        txn.flush()
        var last: Entity? = null
        val sorted = txn.sort("Issue", "size", true)
        Assert.assertEquals(200, sorted.size().toInt().toLong())
        for (entity in sorted) {
            if (last != null) {
                val int1 = last.getProperty("size")
                val int2 = entity.getProperty("size") as Int?
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1 <= int2)
                }
            }
            last = entity
        }
    }

    fun testReverseSort() {
        val txn = storeTransaction
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", 100 - i)
        }
        for (i in 0..99) {
            txn.newEntity("Issue")
        }
        txn.flush()
        var last: Entity? = null
        val sorted = txn.sort("Issue", "size", false)
        Assert.assertEquals(200, sorted.size().toInt().toLong())
        for (entity in sorted) {
            if (last != null) {
                val int1 = last.getProperty("size")
                val int2 = entity.getProperty("size") as Int?
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1 >= int2)
                }
            }
            last = entity
        }
    }

    fun testReverseSort2() {
        val txn = storeTransaction
        for (i in 0..9) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", (15 - i) / 5)
        }
        txn.flush()
        var last: Entity? = null
        val sorted = txn.sort("Issue", "size", false)
        Assert.assertEquals(10, sorted.size().toInt().toLong())
        for (entity in sorted) {
            if (last != null) {
                val int1 = last.getProperty("size")
                val int2 = entity.getProperty("size") as Int?
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1 >= int2)
                }
            }
            last = entity
        }
    }

    fun testReverseSort_XD_175() {
        val txn = storeTransaction
        for (i in 0..9) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", i)
        }
        txn.flush()
        var last: Entity? = null
        val sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), false)
        Assert.assertEquals(10, sorted.size().toInt().toLong())
        for (entity in sorted) {
            if (last != null) {
                val int1 = last.getProperty("size")
                val int2 = entity.getProperty("size") as Int?
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1 >= int2)
                }
            }
            last = entity
        }
    }

    fun testNonStableSort() {
        val txn = storeTransaction
        val project = txn.newEntity("Project")
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("body", i.toString())
            issue.setProperty("size", 100 - i)
            if (i < 50) {
                project.addLink("issue", issue)
            }
        }
        for (i in 0..99) {
            project.addLink("issue", txn.newEntity("Issue"))
        }
        txn.flush()
        var last: Entity? = null
        var sorted = txn.sort("Issue", "body", project.getLinks("issue"), true)
        Assert.assertEquals(150, sorted.size().toInt().toLong())
        for (entity in sorted) {
            if (last != null) {
                val stringProp = last.getProperty("body")
                val s = entity.getProperty("body") as String?
                if (stringProp == null) {
                    Assert.assertNull(s)
                } else if (s != null) {
                    Assert.assertTrue(stringProp <= s)
                }
            }
            last = entity
        }
        sorted = txn.sort("Issue", "size", project.getLinks("issue"), true)
        Assert.assertEquals(150, sorted.size().toInt().toLong())
        last = null
        for (entity in sorted) {
            if (last != null) {
                val intProp = last.getProperty("size")
                val i = entity.getProperty("size") as Int?
                if (intProp == null) {
                    Assert.assertNull(i)
                } else if (i != null) {
                    Assert.assertTrue(intProp <= i)
                }
            }
            last = entity
        }
    }

    fun testSortWithNullValues() {
        val txn = storeTransaction
        for (i in 0..9) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #$i")
        }
        for (i in 0..9) {
            txn.newEntity("Issue")
        }
        txn.flush()
        var sorted = txn.sort("Issue", "description", true)
        Assert.assertEquals(20, sorted.size().toInt().toLong())
        var it = sorted.iterator()
        Assert.assertTrue(it.hasNext())
        var next = it.next()
        Assert.assertNotNull(next)
        Assert.assertNotNull(next.getProperty("description"))
        sorted = txn.sort("Issue", "description", false)
        Assert.assertEquals(20, sorted.size().toInt().toLong())
        it = sorted.iterator()
        Assert.assertTrue(it.hasNext())
        next = it.next()
        Assert.assertNotNull(next)
        Assert.assertNotNull(next.getProperty("description"))
    }

    fun testStableSortPropertyValueIterator() {
        val txn = storeTransaction
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("size", i)
        }
        txn.flush()
        val sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), true)
        val it = sorted.iterator() as PropertyValueIterator
        for (i in 0..99) {
            Assert.assertTrue(it.hasNext())
            Assert.assertEquals(i, it.currentValue())
            Assert.assertNotNull(it.next())
        }
        Assert.assertFalse(it.hasNext())
        Assert.assertNull(it.currentValue())
    }

    fun testStableSortPropertyValueIteratorWithDups() {
        val txn = storeTransaction
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("size", i / 10)
        }
        txn.flush()
        val sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), true)
        val it = sorted.iterator() as PropertyValueIterator
        for (i in 0..99) {
            Assert.assertTrue(it.hasNext())
            Assert.assertEquals(i / 10, it.currentValue())
            Assert.assertNotNull(it.next())
        }
        Assert.assertFalse(it.hasNext())
        Assert.assertNull(it.currentValue())
    }

    fun testStableSortPropertyValueIteratorWithNulls() {
        val txn = storeTransaction
        for (i in 0..98) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("size", i)
        }
        txn.newEntity("Issue")
        txn.flush()
        val sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), true)
        val it = sorted.iterator() as PropertyValueIterator
        for (i in 0..98) {
            Assert.assertTrue(it.hasNext())
            Assert.assertEquals(i, it.currentValue())
            Assert.assertNotNull(it.next())
        }
        Assert.assertTrue(it.hasNext())
        Assert.assertNull(it.currentValue())
        Assert.assertNotNull(it.next())
        Assert.assertFalse(it.hasNext())
        Assert.assertNull(it.currentValue())
    }

    fun testPropertiesIteratorValueInterned() {
        val txn = storeTransaction
        repeat(10) {
            txn.newEntity("Issue").setProperty("summary", "summary0")
        }
        repeat(10) {
            txn.newEntity("Issue").setProperty("summary", "summary1")
        }
        txn.flush()
        val it = txn.findWithPropSortedByValue("Issue", "summary").iterator() as PropertyValueIterator
        var prev: Comparable<*>? = null
        while (it.hasNext()) {
            val currentValue = it.currentValue()
            if (prev == currentValue) {
                Assert.assertTrue(prev === currentValue)
            } else {
                prev = currentValue
            }
            it.next()
        }
    }

    fun testPropertiesIterableCachedWrapper() {
        val txn = storeTransaction
        txn.newEntity("Issue")
        val foo = txn.newEntity("Foo")
        foo.setProperty("size", 1) // to allocate propId
        txn.flush()
        val sorted = txn.findWithPropSortedByValue("Issue", "size")
        (sorted as EntityIterableBase).getOrCreateCachedInstance(txn)
    }

    @TestFor(issue = "XD-520")
    fun testInvalidationOfSortResults() {
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        issue.setProperty("description", "description")
        issue.setProperty("created", System.currentTimeMillis())
        txn.flush()
        val sortedByCreated = txn.sort("Issue", "created", txn.find("Issue", "description", "description"), true) as EntityIterableBase
        for (i in 0..9999999) {
            Assert.assertTrue(sortedByCreated.iterator().hasNext())
            Thread.yield()
            if (sortedByCreated.isCached) {
                issue.setProperty("description", "new description")
                txn.flush()
                Assert.assertFalse(sortedByCreated.iterator().hasNext())
                return
            }
        }
        Assert.assertTrue("EntityIterable wasn't cached", false)
    }

    @TestFor(issue = "XD-609")
    fun testSortTinySourceWithLargeIndex() {
        // switch in-memory sort on
        entityStore.config.isDebugAllowInMemorySort = true

        val txn = storeTransaction
        val count = 50000
        for (i in 0 until count) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("body", (i / 1000).toString())
            if (i % 500 == 0) {
                issue.setProperty("hasComment", true)
            }
        }
        txn.flush()
        println("Sorting started")
        val start = System.currentTimeMillis()
        val unsorted = txn.findWithProp("Issue", "hasComment")
        val sorted = txn.sort("Issue", "body", unsorted, true)
        Assert.assertEquals("9", sorted.last!!.getProperty("body"))
        Assert.assertEquals("0", sorted.first!!.getProperty("body"))
        Assert.assertEquals("0", txn.sort("Issue", "no prop", sorted, true).first!!.getProperty("body"))
        println("Sorting took " + (System.currentTimeMillis() - start))
    }

    @TestFor(issue = "XD-670")
    fun testSortTinySourceWithNullPropsWithLargeIndex() {
        // switch in-memory sort on
        entityStore.config.isDebugAllowInMemorySort = true

        val txn = storeTransaction
        val count = 50000
        for (i in 0..4) {
            txn.newEntity("Issue").setProperty("hasComment", true)
        }
        for (i in 0 until count) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("body", i / 5000)
            if (i % 5000 == 0) {
                issue.setProperty("hasComment", true)
            }
        }
        txn.flush()
        println("Sorting started")
        val start = System.currentTimeMillis()
        val unsorted = txn.findWithProp("Issue", "hasComment")
        var sorted: EntityIterable
        sorted = txn.sort("Issue", "body", unsorted, true)
        Assert.assertNull(sorted.last!!.getProperty("body"))
        Assert.assertEquals(0, sorted.first!!.getProperty("body"))
        Assert.assertEquals(0, txn.sort("Issue", "no prop", sorted, true).first!!.getProperty("body"))
        sorted = txn.sort("Issue", "body", unsorted, false)
        Assert.assertNotNull(sorted.first!!.getProperty("body"))
        println("Sorting took " + (System.currentTimeMillis() - start))
    }

    @TestFor(issues = ["XD-670, XD-736"])
    fun testSortTinySourceWithPropsWithLargeIndexStability() {
        // switch in-memory sort on
        entityStore.config.isDebugAllowInMemorySort = true

        val txn = storeTransaction
        val count = 50000
        val divBuckets = 3
        for (i in 0 until count) {
            val issue = txn.newEntity("Issue")
            if (i % 4000 == 0) {
                if (i < count / 4) {
                    issue.setProperty("body", i)
                } else {
                    issue.setProperty("body", count / 2 - i) // flip most of the data
                }
            }
            issue.setProperty("div", i % divBuckets)
        }
        txn.flush()
        txn.findWithPropSortedByValue("Issue", "body").size()
        txn.findWithProp("Issue", "div").size()
        entityStore.asyncProcessor.waitForJobs(100)
        val firstSorted = txn.findWithPropSortedByValue("Issue", "body")
        val sortedNonStable = txn.sort("Issue", "div", firstSorted, true)
        sortedNonStable.last
        entityStore.asyncProcessor.waitForJobs(100)
        val sortedStable = txn.sort("Issue", "div", firstSorted.asSortResult(), true)
        var prev: Entity? = null
        var buckets = 1
        for (entity in sortedStable) {
            prev = if (prev == null) {
                entity
            } else {
                val prevDiv = prev.getProperty("div") as Int
                val div = entity.getProperty("div") as Int
                Assert.assertTrue(prevDiv <= div)
                if (prevDiv == div) {
                    Assert.assertNotEquals(prev, entity)
                    Assert.assertTrue(prev.getProperty("body") as Int <= entity.getProperty("body") as Int)
                } else {
                    buckets++
                }
                entity
            }
        }
        Assert.assertEquals(divBuckets.toLong(), buckets.toLong())
    }

    fun testSortByTwoColumnsAscendingStable() {
        sortByTwoColumns(true, true)
    }

    fun testSortByTwoColumnsDescendingStable() {
        sortByTwoColumns(true, false)
    }

    fun testSortByTwoColumnsAscendingNonStable() {
        sortByTwoColumns(false, true)
    }

    fun testSortByTwoColumnsDescendingNonStable() {
        sortByTwoColumns(false, false)
    }

    private fun sortByTwoColumns(stable: Boolean, asc: Boolean) {
        val txn = storeTransaction
        val count = 5000
        for (i in 0 until count) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("body", (i / 5).toString())
            issue.setProperty("size", 100 - i)
        }
        for (i in 0 until count) {
            txn.newEntity("Issue")
        }
        txn.flush()
        println("Sorting started")
        val start = System.currentTimeMillis()
        var source = txn.sort("Issue", "size", asc)
        if (stable) {
            source = source.asSortResult()
        }
        val sorted = txn.sort("Issue", "body", source, true)
        println("Sorting calculation took " + (System.currentTimeMillis() - start))
        Assert.assertEquals((2 * count).toLong(), sorted.size().toInt().toLong())
        var last: Entity? = null
        for (issue in sorted) {
            if (last != null) {
                val str1 = last.getProperty("body")
                val str2 = issue.getProperty("body") as String?
                if (str1 != null && str2 != null) {
                    val bodycmp = str1.compareTo(str2)
                    Assert.assertTrue(bodycmp <= 0)
                    val intProp = last.getProperty("size")
                    Assert.assertNotNull(intProp)
                    val sizecmp = intProp!!.compareTo((issue.getProperty("size") as Int?)!!)
                    if (stable && bodycmp == 0) {
                        Assert.assertEquals(asc, sizecmp < 0)
                    }
                }
            }
            last = issue
        }
        println("Sorting took " + (System.currentTimeMillis() - start))
    }
}
