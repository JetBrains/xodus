/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
import jetbrains.exodus.bindings.ComparableSet;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class FindTests extends EntityStoreTestBase {

    public void testFindSingleEntityByPropertyValue() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i);
            entity.setProperty("size", i);
        }
        txn.flush();
        // find issues with size 50
        final EntityIterable issues = txn.find("Issue", "size", 50);
        int count = 0;
        for (final Entity issue : issues) {
            Assert.assertEquals(50, issue.getProperty("size"));
            count++;
        }
        Assert.assertEquals(1, count);
    }

    public void testFindByStringPropertyValue() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i % 10);
            entity.setProperty("size", i);
        }
        txn.flush();
        final EntityIterable issues = txn.find("Issue", "description", "Test issue #5");
        int count = 0;
        for (final Entity issue : issues) {
            Assert.assertEquals("Test issue #5", issue.getProperty("description"));
            count++;
        }
        Assert.assertEquals(10, count);
    }

    public void testFindByStringPropertyValueIgnoreCase() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i % 10);
            entity.setProperty("size", i);
        }
        txn.flush();
        final EntityIterable issues = txn.find("Issue", "description", "Test ISSUE #5");
        int count = 0;
        for (final Entity issue : issues) {
            Assert.assertEquals("Test issue #5", issue.getProperty("description"));
            count++;
        }
        Assert.assertEquals(10, count);
    }

    public void testSingularFind() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity entity = txn.newEntity("Issue");
        entity.setProperty("name", "noname");
        entity.setProperty("size", 6);
        txn.flush();
        Assert.assertEquals(0, (int) txn.find("Issue", "name", "thename").size());
        Assert.assertEquals(0, (int) txn.find("Issue", "name", 6).size());
        Assert.assertEquals(0, (int) txn.find("Issue", "size", "wtf").size());
        Assert.assertEquals(0, (int) txn.find("Issue", "description", "Test Issue").size());
    }

    public void testFindByPropAfterSeveralTxns() {
        final int pi = 31415926;
        final int e = 271828182;
        final StoreTransaction txn = getStoreTransaction();
        Entity issue = txn.newEntity("Issue");
        issue.setProperty("size", pi);
        txn.flush();
        Assert.assertEquals(1, (int) txn.find("Issue", "size", pi).size());
        issue = txn.newEntity("Issue");
        issue.setProperty("size", pi);
        txn.flush();
        Assert.assertEquals(2, (int) txn.find("Issue", "size", pi).size());
        issue = txn.newEntity("Issue");
        issue.setProperty("size", pi);
        txn.flush();
        Assert.assertEquals(3, (int) txn.find("Issue", "size", pi).size());
        issue = txn.newEntity("Issue");
        issue.setProperty("size", e);
        txn.flush();
        Assert.assertEquals(3, (int) txn.find("Issue", "size", pi).size());
        Assert.assertEquals(1, (int) txn.find("Issue", "size", e).size());
        issue = txn.find("Issue", "size", pi).iterator().next();
        issue.setProperty("size", e);
        txn.flush();
        Assert.assertEquals(2, (int) txn.find("Issue", "size", pi).size());
        Assert.assertEquals(2, (int) txn.find("Issue", "size", e).size());
        issue = txn.find("Issue", "size", pi).iterator().next();
        issue.setProperty("size", e);
        txn.flush();
        Assert.assertEquals(1, (int) txn.find("Issue", "size", pi).size());
        Assert.assertEquals((int) txn.find("Issue", "size", e).size(), 3);
        issue = txn.find("Issue", "size", pi).iterator().next();
        issue.setProperty("size", e);
        txn.flush();
        Assert.assertEquals(0, (int) txn.find("Issue", "size", pi).size());
        Assert.assertEquals(4, (int) txn.find("Issue", "size", e).size());
    }

    public void testFindByPropAfterSeveralTxns2() {
        final int pi = 31415926;
        final int e = 271828182;
        final StoreTransaction txn = getStoreTransaction();
        Entity issue1 = txn.newEntity("Issue");
        Entity issue2 = txn.newEntity("Issue");
        issue1.setProperty("size", pi);
        issue2.setProperty("size", e);
        txn.flush();
        Assert.assertEquals(0, (int) txn.find("Issue", "size", 0).size());
        Assert.assertEquals(1, (int) txn.find("Issue", "size", pi).size());
        Assert.assertEquals(1, (int) txn.find("Issue", "size", e).size());
        for (int i = 0; i < 101; ++i) {
            int t = (System.currentTimeMillis() & 1) == 1 ? pi : e;
            issue1.setProperty("size", t);
            issue2.setProperty("size", t == pi ? e : pi);
            txn.flush();
            Assert.assertEquals(0, (int) txn.find("Issue", "size", 0).size());
            Assert.assertEquals(1, (int) txn.find("Issue", "size", pi).size());
            Assert.assertEquals(1, (int) txn.find("Issue", "size", e).size());
        }
    }

    public void testCreateCheckSize() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals("Iteration " + i, (long) i, txn.getAll("Issue").size());
            txn.newEntity("Issue");
            txn.flush();
            Assert.assertEquals("Iteration " + i, (long) (i + 1), txn.getAll("Issue").size());
        }
    }

    public void testCreateFindByPropValue() {
        final StoreTransaction txn = getStoreTransaction();
        Entity e = txn.newEntity("Issue");
        e.setProperty("size", "S" + 0);
        txn.flush();
        for (int i = 0; i < 1000; ++i) {
            if (!txn.find("Issue", "size", "s" + i).iterator().hasNext()) {
                e = txn.newEntity("Issue");
                e.setProperty("size", "S" + i);
                txn.flush();
            }
            final EntityIterable it = txn.find("Issue", "size", "s" + i);
            if (!it.iterator().hasNext()) {
                throw new RuntimeException("Iteration " + i + ", it " + it.toString());
            }
        }
    }

    public void testCreateFindByPropValueReverseOrder() {
        final StoreTransaction txn = getStoreTransaction();
        Entity e = txn.newEntity("Issue");
        e.setProperty("size", "S" + 0);
        txn.flush();
        for (int i = 1000; i >= 0; --i) {
            if (!txn.find("Issue", "size", "s" + i).iterator().hasNext()) {
                e = txn.newEntity("Issue");
                e.setProperty("size", "S" + i);
                txn.flush();
            }
            final EntityIterable it = txn.find("Issue", "size", "s" + i);
            if (!it.iterator().hasNext()) {
                throw new RuntimeException("Iteration " + i + ", it " + it.toString());
            }
        }
    }

    public void testCreateFindByPropValueAndIntersect() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        Entity e = txn.newEntity("Issue");
        Entity owner = txn.newEntity("User");
        e.setProperty("size", Integer.toString(0));
        e.addLink("owner", owner);
        txn.flush();
        for (int i = 0; i < 1000; ++i) {
            if (i % 1000 == 0) {
                System.out.println("Iteration: " + i);
            }
            EntityIterable it = txn.find("Issue", "size", Integer.toString(i)).intersect(txn.findLinks("Issue", owner, "owner"));
            if (!it.iterator().hasNext()) {
                e = txn.newEntity("Issue");
                e.setProperty("size", Integer.toString(i));
                e.addLink("owner", owner);
                txn.flush();
            }
            final EntityIterable links = txn.findLinks("Issue", owner, "owner");
            it = txn.find("Issue", "size", Integer.toString(i)).intersect(links);
            final EntityIterator itt = it.iterator();
            if (!itt.hasNext()) {
                throw new RuntimeException("0: Iteration " + i + ", it " + it.toString() + ", links " + links.toString());
            }
            Assert.assertEquals(e, itt.next());
            if (itt.hasNext()) {
                throw new RuntimeException("2: Iteration " + i + ", it " + it.toString() + ", links " + links.toString());
            }
        }
    }

    public void testCreateFindByPropValueAndIntersectReverseOrder() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        Entity e = txn.newEntity("Issue");
        Entity eSaved = e;
        Entity owner = txn.newEntity("User");
        e.setProperty("size", Integer.toString(0));
        e.addLink("owner", owner);
        txn.flush();
        for (int i = 1000; i >= 0; --i) {
            EntityIterable it = txn.find("Issue", "size", Integer.toString(i)).intersect(txn.findLinks("Issue", owner, "owner"));
            if (!it.iterator().hasNext()) {
                e = txn.newEntity("Issue");
                e.setProperty("size", Integer.toString(i));
                e.addLink("owner", owner);
                txn.flush();
            } else {
                e = eSaved;
            }
            final EntityIterable links = txn.findLinks("Issue", owner, "owner");
            it = txn.find("Issue", "size", Integer.toString(i)).intersect(links);
            final EntityIterator itt = it.iterator();
            if (!itt.hasNext()) {
                throw new RuntimeException("0: Iteration " + i + ", it " + it.toString() + ", links " + links.toString());
            }
            Assert.assertEquals(e, itt.next());
            if (itt.hasNext()) {
                throw new RuntimeException("2: Iteration " + i + ", it " + it.toString() + ", links " + links.toString());
            }
        }
    }

    public void testOrderByEntityId() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final List<EntityId> ids = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            ids.add(txn.newEntity("Issue").getId());
        }
        txn.flush();
        // reverse the order of created ids
        for (int i = 0, j = ids.size() - 1; i < j; ++i, --j) {
            final EntityId id = ids.get(i);
            ids.set(i, ids.get(j));
            ids.set(j, id);
        }
        for (final EntityId id : ids) {
            final Entity e = txn.getEntity(id);
            if (e != null) {
                e.setProperty("description", "Test issue");
            }
            txn.flush();
        }
        final EntityIterable issues = txn.find("Issue", "description", "Test issue");
        long localId = 0;
        for (final Entity issue : issues) {
            Assert.assertEquals(localId, issue.getId().getLocalId()); // correct order
            ++localId;
        }
        Assert.assertEquals(ids.size(), (int) localId);
    }

    public void testFindRange() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i % 10);
            entity.setProperty("size", i);
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.find("Issue", "size", 0, 100).size());
        Assert.assertEquals(11, (int) txn.find("Issue", "size", 0, 10).size());
        Assert.assertEquals(10, (int) txn.find("Issue", "size", 90, 100).size());
    }

    public void testFindRangeOrder() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("description", "Test issue #" + i % 10);
            issue.setProperty("size", 99 - i);
        }
        txn.flush();
        int i = 0;
        for (final Entity issue : txn.find("Issue", "size", 0, 100)) {
            Assert.assertEquals(i++, issue.getProperty("size"));
        }
    }

    public void testFindRangeByStrings() {
        final StoreTransaction txn = getStoreTransaction();
        Entity entity = txn.newEntity("Issue");
        entity.setProperty("description", "aaa");
        entity = txn.newEntity("Issue");
        entity.setProperty("description", "bbb");
        entity = txn.newEntity("Issue");
        entity.setProperty("description", "ccc");
        entity = txn.newEntity("Issue");
        entity.setProperty("description", "dddd");
        txn.flush();
        Assert.assertEquals(4, (int) txn.find("Issue", "description", "a", "e").size());
        Assert.assertEquals(3, (int) txn.find("Issue", "description", "b", "e").size());
        Assert.assertEquals(3, (int) txn.find("Issue", "description", "a", "d").size());
        Assert.assertEquals(2, (int) txn.find("Issue", "description", "a", "c").size());
        Assert.assertEquals(2, (int) txn.find("Issue", "description", "b", "d").size());
        Assert.assertEquals(2, (int) txn.find("Issue", "description", "c", "e").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "a", "b").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "d", "e").size());
        Assert.assertEquals(0, (int) txn.find("Issue", "description", "a", "a").size());
    }

    public void testFindRangeByStringsIgnoreCase() {
        final StoreTransaction txn = getStoreTransaction();
        Entity entity = txn.newEntity("Issue");
        entity.setProperty("description", "aaa");
        entity = txn.newEntity("Issue");
        entity.setProperty("description", "bbb");
        entity = txn.newEntity("Issue");
        entity.setProperty("description", "ccc");
        entity = txn.newEntity("Issue");
        entity.setProperty("description", "dddd");
        txn.flush();
        Assert.assertEquals(4, (int) txn.find("Issue", "description", "a", "E").size());
        Assert.assertEquals(3, (int) txn.find("Issue", "description", "B", "e").size());
        Assert.assertEquals(3, (int) txn.find("Issue", "description", "a", "D").size());
        Assert.assertEquals(2, (int) txn.find("Issue", "description", "A", "c").size());
        Assert.assertEquals(2, (int) txn.find("Issue", "description", "B", "D").size());
        Assert.assertEquals(2, (int) txn.find("Issue", "description", "C", "E").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "A", "B").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "D", "E").size());
        Assert.assertEquals(0, (int) txn.find("Issue", "description", "a", "A").size());
    }

    public void testFindRangeAddedInBackOrder() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i % 10);
            entity.setProperty("size", 99 - i);
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.find("Issue", "size", 0, 100).size());
        Assert.assertEquals(11, (int) txn.find("Issue", "size", 0, 10).size());
        Assert.assertEquals(10, (int) txn.find("Issue", "size", 90, 100).size());
    }

    public void testSingularFindRangeByStrings() {
        final StoreTransaction txn = getStoreTransaction();
        Entity entity = txn.newEntity("Issue");
        entity.setProperty("description", "a");
        txn.flush();
        Assert.assertEquals(0, (int) txn.find("Issue", "description", "e", "a").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "a", "a").size());
        Assert.assertEquals(0, (int) txn.find("Issue", "description", "E", "A").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "A", "A").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "a", "A").size());
        Assert.assertEquals(1, (int) txn.find("Issue", "description", "A", "a").size());
        Assert.assertEquals(0, (int) txn.find("Issue", "size", Integer.MIN_VALUE, Integer.MAX_VALUE).size());
    }

    public void testStartsWith() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i);
        }
        txn.flush();
        Assert.assertEquals(0, (int) txn.findStartingWith("Issue", "description", "a").size());
        Assert.assertEquals(11, (int) txn.findStartingWith("Issue", "description", "Test issue #1").size());
        Assert.assertEquals(11, (int) txn.findStartingWith("Issue", "description", "Test issue #5").size());
        Assert.assertEquals(11, (int) txn.findStartingWith("Issue", "description", "Test issue #9").size());
        Assert.assertEquals(100, (int) txn.findStartingWith("Issue", "description", "Test issue #").size());
        Assert.assertEquals(100, (int) txn.findStartingWith("Issue", "description", "Test").size());
    }

    public void testFindWithProp() throws Exception {
        testFindSingleEntityByPropertyValue();
        final StoreTransaction txn = getStoreTransaction();
        Assert.assertEquals(100, txn.findWithProp("Issue", "description").size());
        Assert.assertEquals(100, txn.findWithProp("Issue", "size").size());
        Assert.assertEquals(0, txn.findWithProp("Issue", "no such property").size());
        Assert.assertEquals(0, txn.findWithProp("No such type", "size").size());
    }

    public void testFindWithPropSorted() throws Exception {
        testFindSingleEntityByPropertyValue();
        final PersistentStoreTransaction txn = getStoreTransaction();
        Assert.assertEquals(100, txn.findWithPropSortedByValue("Issue", "description").size());
        final PersistentEntity nonExistent = new PersistentEntity(getEntityStore(), new PersistentEntityId(111, 0));
        Assert.assertEquals(-1, txn.findWithPropSortedByValue("Issue", "description").indexOf(nonExistent));
        Assert.assertEquals(100, txn.findWithPropSortedByValue("Issue", "size").size());
        Assert.assertEquals(0, txn.findWithPropSortedByValue("Issue", "no such property").size());
        Assert.assertEquals(0, txn.findWithPropSortedByValue("No such type", "size").size());
    }

    public void testFindWithPropIsCached() throws Exception {
        getEntityStore().getConfig().setCachingDisabled(false);
        testFindWithProp();
        final StoreTransaction txn = getStoreTransaction();
        Assert.assertTrue(((EntityIteratorBase) txn.findWithProp("Issue", "description").iterator()).getIterable().isCachedInstance());
    }

    public void testFindWithPropSortedIsCached() throws Exception {
        getEntityStore().getConfig().setCachingDisabled(false);
        testFindWithPropSorted();
        final PersistentStoreTransaction txn = getStoreTransaction();
        Assert.assertTrue(((EntityIteratorBase) txn.findWithPropSortedByValue("Issue", "description").iterator()).getIterable().isCachedInstance());
    }

    @TestFor(issues = "XD-524")
    public void testFindWithPropAndIntersectIsCached() throws Exception {
        testFindWithPropSortedIsCached();
        final PersistentStoreTransaction txn = getStoreTransaction();
        EntityIterableBase withDescription = txn.findWithPropSortedByValue("Issue", "description");
        withDescription = getEntityStore().getEntityIterableCache().putIfNotCached(withDescription);
        Assert.assertTrue(((EntityIteratorBase) withDescription.iterator()).getIterable().isCachedInstance());
        final EntityIterableBase intersect = (EntityIterableBase) withDescription.intersect(txn.findWithProp("Issue", "size"));
        Assert.assertTrue(intersect.canBeCached());
        Assert.assertEquals(100, intersect.size());
        Assert.assertTrue(((EntityIteratorBase) intersect.iterator()).getIterable().isCachedInstance());
    }

    @TestFor(issues = "XD-524")
    public void testFindWithPropAndUnionIsCached() throws Exception {
        testFindWithPropSortedIsCached();
        final PersistentStoreTransaction txn = getStoreTransaction();
        EntityIterableBase withDescription = txn.findWithPropSortedByValue("Issue", "description");
        withDescription = getEntityStore().getEntityIterableCache().putIfNotCached(withDescription);
        Assert.assertTrue(((EntityIteratorBase) withDescription.iterator()).getIterable().isCachedInstance());
        final EntityIterableBase union = (EntityIterableBase) withDescription.union(txn.findWithProp("Issue", "size"));
        Assert.assertTrue(union.canBeCached());
        Assert.assertEquals(100, union.size());
        Assert.assertTrue(((EntityIteratorBase) union.iterator()).getIterable().isCachedInstance());
    }

    @TestFor(issues = "XD-524")
    public void testFindWithPropAndMinusIsCached() throws Exception {
        testFindWithPropSortedIsCached();
        final PersistentStoreTransaction txn = getStoreTransaction();
        EntityIterableBase withDescription = txn.findWithPropSortedByValue("Issue", "description");
        withDescription = getEntityStore().getEntityIterableCache().putIfNotCached(withDescription);
        Assert.assertTrue(((EntityIteratorBase) withDescription.iterator()).getIterable().isCachedInstance());
        txn.getAll("Issue").getFirst().setProperty("created", System.currentTimeMillis());
        txn.flush();
        final EntityIterableBase minus = (EntityIterableBase) withDescription.minus(txn.findWithProp("Issue", "created"));
        Assert.assertTrue(minus.canBeCached());
        Assert.assertEquals(99, minus.size());
        Assert.assertTrue(((EntityIteratorBase) minus.iterator()).getIterable().isCachedInstance());
    }

    public void testFindWithBlob() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setBlobString("description", "Test issue #" + i);
            entity.setProperty("size", i);
        }
        txn.flush();
        Assert.assertEquals(100, txn.findWithBlob("Issue", "description").size());
        Assert.assertEquals(0, txn.findWithBlob("Issue", "no such blob").size());
    }

    public void testFindComparableSet() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        final ComparableSet<Integer> randomSet = new ComparableSet<>();
        final Random rnd = new Random();
        final int setSize = 20;
        for (int i = 0; i < setSize; ++i) {
            randomSet.addItem(rnd.nextInt());
        }
        for (int i = 0; i < 1000; ++i) {
            Assert.assertEquals(setSize, randomSet.size());
            issue.setProperty("randomSet", randomSet);
            randomSet.forEach(new ComparableSet.Consumer<Integer>() {
                @Override
                public void accept(@NotNull final Integer item, int index) {
                    Assert.assertEquals(issue, txn.find("Issue", "randomSet", item).getFirst());
                }
            });
            Assert.assertTrue(randomSet.removeItem(randomSet.iterator().next()));
            while (true) {
                final int newItem = rnd.nextInt();
                if (randomSet.addItem(newItem)) {
                    Assert.assertTrue(txn.find("Issue", "randomSet", newItem).isEmpty());
                    break;
                }
            }
            if (i % 20 == 19) {
                txn.flush();
            }
        }
    }

    @TestFor(issues = "XD-510")
    public void testFindComparableSetRange() throws InterruptedException {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        final ComparableSet<String> set = new ComparableSet<>();
        set.addItem("Eugene");

        issue.setProperty("commenters", set);
        txn.flush();

        for (int i = 0; i < 20; ++i) {
            Assert.assertEquals(issue, txn.findStartingWith("Issue", "commenters", "eug").getFirst());
            set.addItem("" + i);
            issue.setProperty("commenters", set);
            txn.flush();
            Thread.sleep(20);
        }
    }

    @TestFor(issues = "XD-511")
    public void testFindComparableSetCaseInsensitive() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        final ComparableSet<String> set = new ComparableSet<>();
        set.addItem("Eugene");
        set.addItem("MAX");
        set.addItem("SlaVa");
        set.addItem("Pavel");
        set.addItem("AnnA");

        issue.setProperty("commenters", set);
        txn.flush();

        Assert.assertEquals(issue, txn.find("Issue", "commenters", "eugene").getFirst());
        Assert.assertEquals(issue, txn.find("Issue", "commenters", "Max").getFirst());
        Assert.assertEquals(issue, txn.find("Issue", "commenters", "slaVa").getFirst());
        Assert.assertEquals(issue, txn.findStartingWith("Issue", "commenters", "Pav").getFirst());
        Assert.assertEquals(issue, txn.findStartingWith("Issue", "commenters", "ann").getFirst());
    }

    @TestFor(issues = "XD-512")
    public void testComparableSetPropertiesIterable() {
        getEntityStore().getConfig().setCachingDisabled(true); // disable caching to avoid background exceptions
        testFindComparableSetCaseInsensitive();
        final PersistentStoreTransaction txn = getStoreTransaction();
        Assert.assertTrue(txn.findWithPropSortedByValue("Issue", "commenters").iterator().hasNext());
    }

    @TestFor(issues = "XD-577")
    public void testSuccessiveInvalidationAndUpdateCachedResult() throws InterruptedException {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        issue.setProperty("summary", "summary");
        Assert.assertEquals(1L, txn.findStartingWith("Issue", "summary", "summary").size());
        issue.setProperty("summary", "no summary");
        Assert.assertEquals(0L, txn.findStartingWith("Issue", "summary", "summary").size());
        issue.setProperty("summary", "summary");
        Assert.assertEquals(1L, txn.findStartingWith("Issue", "summary", "summary").size());
    }

    @TestFor(issues = "XD-618")
    public void testInvalidateComparableSetPropertyIterables() {
        testFindComparableSetCaseInsensitive();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
        }

        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.getAll("Issue").getFirst();
        final ComparableSet<String> set = (ComparableSet<String>) issue.getProperty("commenters");
        set.addItem("bot");

        issue.setProperty("commenters", set);
    }
}
