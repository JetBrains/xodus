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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.TestFor;
import jetbrains.exodus.entitystore.*;
import org.junit.Assert;

@SuppressWarnings({"unchecked"})
public class SortTests extends EntityStoreTestBase {

    public void testSort() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", 100 - i);
        }
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Entity last = null;
        EntityIterable sorted = txn.sort("Issue", "size", true);
        Assert.assertEquals(200, (int) sorted.size());
        for (Entity entity : sorted) {
            if (last != null) {
                Comparable<Integer> int1 = last.getProperty("size");
                final Integer int2 = (Integer) entity.getProperty("size");
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1.compareTo(int2) <= 0);
                }
            }
            last = entity;
        }
    }

    public void testReverseSort() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", 100 - i);
        }
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Entity last = null;
        EntityIterable sorted = txn.sort("Issue", "size", false);
        Assert.assertEquals(200, (int) sorted.size());
        for (Entity entity : sorted) {
            if (last != null) {
                Comparable<Integer> int1 = last.getProperty("size");
                final Integer int2 = (Integer) entity.getProperty("size");
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1.compareTo(int2) >= 0);
                }
            }
            last = entity;
        }
    }

    public void testReverseSort2() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 10; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", (15 - i) / 5);
        }
        txn.flush();
        Entity last = null;
        EntityIterable sorted = txn.sort("Issue", "size", false);
        Assert.assertEquals(10, (int) sorted.size());
        for (Entity entity : sorted) {
            if (last != null) {
                Comparable<Integer> int1 = last.getProperty("size");
                final Integer int2 = (Integer) entity.getProperty("size");
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1.compareTo(int2) >= 0);
                }
            }
            last = entity;
        }
    }

    public void testReverseSort_XD_175() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 10; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", i);
        }
        txn.flush();
        Entity last = null;
        EntityIterable sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), false);
        Assert.assertEquals(10, (int) sorted.size());
        for (Entity entity : sorted) {
            if (last != null) {
                Comparable<Integer> int1 = last.getProperty("size");
                final Integer int2 = (Integer) entity.getProperty("size");
                if (int1 != null && int2 != null) {
                    Assert.assertTrue(int1.compareTo(int2) >= 0);
                }
            }
            last = entity;
        }
    }

    public void testNonStableSort() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity project = txn.newEntity("Project");
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("body", Integer.toString(i));
            issue.setProperty("size", 100 - i);
            if (i < 50) {
                project.addLink("issue", issue);
            }
        }
        for (int i = 0; i < 100; ++i) {
            project.addLink("issue", txn.newEntity("Issue"));
        }
        txn.flush();
        Entity last = null;
        EntityIterable sorted = txn.sort("Issue", "body", project.getLinks("issue"), true);
        Assert.assertEquals(150, (int) sorted.size());
        for (Entity entity : sorted) {
            if (last != null) {
                final Comparable<String> stringProp = last.getProperty("body");
                final String s = (String) entity.getProperty("body");
                if (stringProp == null) {
                    Assert.assertNull(s);
                } else if (s != null) {
                    Assert.assertTrue(stringProp.compareTo(s) <= 0);
                }
            }
            last = entity;
        }
        sorted = txn.sort("Issue", "size", project.getLinks("issue"), true);
        Assert.assertEquals(150, (int) sorted.size());
        last = null;
        for (Entity entity : sorted) {
            if (last != null) {
                final Comparable<Integer> intProp = last.getProperty("size");
                final Integer i = (Integer) entity.getProperty("size");
                if (intProp == null) {
                    Assert.assertNull(i);
                } else if (i != null) {
                    Assert.assertTrue(intProp.compareTo(i) <= 0);
                }
            }
            last = entity;
        }
    }

    public void testSortWithNullValues() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 10; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i);
        }
        for (int i = 0; i < 10; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        EntityIterable sorted = txn.sort("Issue", "description", true);
        Assert.assertEquals(20, (int) sorted.size());
        EntityIterator it = sorted.iterator();
        Assert.assertTrue(it.hasNext());
        Entity next = it.next();
        Assert.assertNotNull(next);
        Assert.assertNotNull(next.getProperty("description"));
        sorted = txn.sort("Issue", "description", false);
        Assert.assertEquals(20, (int) sorted.size());
        it = sorted.iterator();
        Assert.assertTrue(it.hasNext());
        next = it.next();
        Assert.assertNotNull(next);
        Assert.assertNotNull(next.getProperty("description"));
    }

    public void testStableSortPropertyValueIterator() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("size", i);
        }
        txn.flush();
        final EntityIterable sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), true);
        final PropertyValueIterator it = (PropertyValueIterator) sorted.iterator();
        for (int i = 0; i < 100; ++i) {
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(i, it.currentValue());
            Assert.assertNotNull(it.next());
        }
        Assert.assertFalse(it.hasNext());
        Assert.assertNull(it.currentValue());
    }

    public void testStableSortPropertyValueIteratorWithDups() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("size", i / 10);
        }
        txn.flush();
        final EntityIterable sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), true);
        final PropertyValueIterator it = (PropertyValueIterator) sorted.iterator();
        for (int i = 0; i < 100; ++i) {
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(i / 10, it.currentValue());
            Assert.assertNotNull(it.next());
        }
        Assert.assertFalse(it.hasNext());
        Assert.assertNull(it.currentValue());
    }

    public void testStableSortPropertyValueIteratorWithNulls() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 99; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("size", i);
        }
        txn.newEntity("Issue");
        txn.flush();
        final EntityIterable sorted = txn.sort("Issue", "size", txn.getAll("Issue").asSortResult(), true);
        final PropertyValueIterator it = (PropertyValueIterator) sorted.iterator();
        for (int i = 0; i < 99; ++i) {
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(i, it.currentValue());
            Assert.assertNotNull(it.next());
        }
        Assert.assertTrue(it.hasNext());
        Assert.assertNull(it.currentValue());
        Assert.assertNotNull(it.next());
        Assert.assertFalse(it.hasNext());
        Assert.assertNull(it.currentValue());
    }

    public void testPropertiesIterableCachedWrapper() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue");
        final Entity foo = txn.newEntity("Foo");
        foo.setProperty("size", 1); // to allocate propId
        txn.flush();
        final EntityIterable sorted = txn.findWithPropSortedByValue("Issue", "size");
        ((EntityIterableBase) sorted).getOrCreateCachedInstance(txn);
    }

    @TestFor(issues = "XD-520")
    public void testInvalidationOfSortResults() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity issue = txn.newEntity("Issue");
        issue.setProperty("description", "description");
        issue.setProperty("created", System.currentTimeMillis());
        txn.flush();
        final EntityIterableBase sortedByCreated =
            (EntityIterableBase) txn.sort("Issue", "created", txn.find("Issue", "description", "description"), true);
        for (; ; ) {
            Assert.assertTrue(sortedByCreated.iterator().hasNext());
            Thread.yield();
            if (sortedByCreated.isCached()) {
                break;
            }
        }
        issue.setProperty("description", "new description");
        txn.flush();
        Assert.assertFalse(sortedByCreated.iterator().hasNext());
    }

    @TestFor(issues = "XD-609")
    public void testSortTinySourceWithLargeIndex() throws InterruptedException {
        // switch in-memory sort on
        getEntityStore().getConfig().setDebugAllowInMemorySort(true);

        final PersistentStoreTransaction txn = getStoreTransaction();
        final int count = 50000;
        for (int i = 0; i < count; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("body", Integer.toString(i / 1000));
            if (i % 500 == 0) {
                issue.setProperty("hasComment", true);
            }
        }
        txn.flush();
        System.out.println("Sorting started");
        final long start = System.currentTimeMillis();
        final EntityIterableBase unsorted = txn.findWithProp("Issue", "hasComment");
        final EntityIterable sorted = txn.sort("Issue", "body", unsorted, true);
        Assert.assertEquals("9", sorted.getLast().getProperty("body"));
        Assert.assertEquals("0", sorted.getFirst().getProperty("body"));
        Assert.assertEquals("0", txn.sort("Issue", "no prop", sorted, true).getFirst().getProperty("body"));
        System.out.println("Sorting took " + (System.currentTimeMillis() - start));
    }

    public void testSortByTwoColumnsAscendingStable() {
        sortByTwoColumns(true, true);
    }

    public void testSortByTwoColumnsDescendingStable() {
        sortByTwoColumns(true, false);
    }

    public void testSortByTwoColumnsAscendingNonStable() {
        sortByTwoColumns(false, true);
    }

    public void testSortByTwoColumnsDescendingNonStable() {
        sortByTwoColumns(false, false);
    }

    private void sortByTwoColumns(boolean stable, boolean asc) {
        final StoreTransaction txn = getStoreTransaction();
        final int count = 5000;
        for (int i = 0; i < count; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("body", Integer.toString(i / 5));
            issue.setProperty("size", 100 - i);
        }
        for (int i = 0; i < count; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        System.out.println("Sorting started");
        final long start = System.currentTimeMillis();
        EntityIterable source = txn.sort("Issue", "size", asc);
        if (stable) {
            source = source.asSortResult();
        }
        final EntityIterable sorted = txn.sort("Issue", "body", source, true);
        System.out.println("Sorting calculation took " + (System.currentTimeMillis() - start));
        Assert.assertEquals(2 * count, (int) sorted.size());
        Entity last = null;
        for (final Entity issue : sorted) {
            if (last != null) {
                final Comparable<String> str1 = last.getProperty("body");
                final String str2 = (String) issue.getProperty("body");
                if (str1 != null && str2 != null) {
                    int bodycmp = str1.compareTo(str2);
                    Assert.assertTrue(bodycmp <= 0);
                    final Comparable<Integer> intProp = last.getProperty("size");
                    Assert.assertNotNull(intProp);
                    int sizecmp = intProp.compareTo((Integer) issue.getProperty("size"));
                    if (stable && bodycmp == 0) {
                        Assert.assertEquals(asc, sizecmp < 0);
                    }
                }
            }
            last = issue;
        }
        System.out.println("Sorting took " + (System.currentTimeMillis() - start));
    }
}
