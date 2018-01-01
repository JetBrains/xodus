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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.TestFor;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class BinaryOperatorsTests extends EntityStoreTestBase {

    public void testIntersect() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("ready", true);
        }
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(0, (int) txn.getAll("Issue").intersect(EntityIterableBase.EMPTY).size());
        Assert.assertEquals(0, (int) EntityIterableBase.EMPTY.intersect(txn.getAll("Issue")).size());
        Assert.assertEquals(100, (int) txn.getAll("Issue").intersect(txn.find("Issue", "ready", Boolean.TRUE)).size());
    }

    public void testIntersectIsCommutative() throws InterruptedException {
        getEntityStore().getConfig().setCachingDisabled(false);
        final StoreTransaction txn = getStoreTransaction();
        final Entity comment = txn.newEntity("Comment");
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setLink("comment", comment);
        }
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.getAll("Issue").intersect(txn.findWithLinks("Issue", "comment")).size());
        Assert.assertTrue(((EntityIteratorBase) txn.getAll("Issue").intersect(
            txn.findWithLinks("Issue", "comment")).iterator()).getIterable().isCachedInstance());
        // commutative intersect will be cached as well
        Assert.assertTrue(((EntityIteratorBase) txn.findWithLinks("Issue", "comment").intersect(
            txn.getAll("Issue")).iterator()).getIterable().isCachedInstance());
    }

    public void testSingularIntersect() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i % 10);
            entity.setProperty("size", i);
        }
        txn.flush();
        int count = 0;
        for (final Entity issue : txn.getAll("Issue").intersect(txn.find("Issue", "size", 0))) {
            Assert.assertEquals(0, issue.getProperty("size"));
            ++count;
        }
        Assert.assertEquals(1, count);
        count = 0;
        for (final Entity issue : txn.getAll("Issue").intersect(txn.find("Issue", "size", 1000))) {
            Assert.assertEquals(0, issue.getProperty("size"));
            ++count;
        }
        Assert.assertEquals(0, count);
    }

    public void testIntersectDifferentTypes() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
            txn.newEntity("Comment");
        }
        txn.flush();
        Assert.assertEquals(0, (int) txn.getAll("Issue").intersect(txn.getAll("Comment")).size());
        Assert.assertEquals(0, (int) txn.getAll("Issue").intersect(txn.getAll("User")).size());
    }

    public void testIntersectUnsorted() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("name", "Test issue #" + (i % 10));
        }
        txn.flush();
        EntityIterableBase found = (EntityIterableBase) txn.find("Issue", "name", "Test issue #1", "Test issue #2 ");
        Assert.assertEquals(20, (int) found.size());
        assertFalse(found.isSortedById());
        assertFalse(found.canBeReordered());
        Assert.assertEquals(20, (int) txn.getAll("Issue").intersect(txn.find("Issue", "name", "Test issue #1", "Test issue #2 ")).size());
    }

    public void testUnion() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
            txn.newEntity("Comment");
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.getAll("Issue").union(EntityIterableBase.EMPTY).size());
        Assert.assertEquals(100, (int) EntityIterableBase.EMPTY.union(txn.getAll("Issue")).size());
        Assert.assertEquals(200, (int) txn.getAll("Issue").union(txn.getAll("Comment")).size());
    }

    public void testUnionIsCommutative() throws InterruptedException {
        getEntityStore().getConfig().setCachingDisabled(false);
        final StoreTransaction txn = getStoreTransaction();
        final Entity comment = txn.newEntity("Comment");
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setLink("comment", comment);
        }
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(200, (int) txn.getAll("Issue").union(txn.findWithLinks("Issue", "comment")).size());
        Assert.assertTrue(((EntityIteratorBase) txn.getAll("Issue").union(
            txn.findWithLinks("Issue", "comment")).iterator()).getIterable().isCachedInstance());
        // commutative union will be cached as well
        Assert.assertTrue(((EntityIteratorBase) txn.findWithLinks("Issue", "comment").union(
            txn.getAll("Issue")).iterator()).getIterable().isCachedInstance());
    }

    public void testSingularUnion() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
            txn.newEntity("Comment");
        }
        txn.flush();
        Assert.assertEquals(0, (int) txn.getAll("Comment").intersect(txn.getAll("Issue")).size());
        Assert.assertEquals(100, (int) txn.getAll("Issue").union(txn.getAll("Comment").intersect(txn.getAll("Issue"))).size());
    }

    public void testUnionUnsorted() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("name", "Test issue #" + (99 - i));
        }
        txn.flush();
        Assert.assertEquals(23, (int) txn.find("Issue", "name", "Test issue #1", "Test issue #3").size());
        Assert.assertEquals(23, (int) txn.find("Issue", "name", "Test issue #2", "Test issue #4").size());
        Assert.assertEquals(34, (int) txn.find("Issue", "name", "Test issue #1", "Test issue #3").union(
            txn.find("Issue", "name", "Test issue #2", "Test issue #4")).size());
    }

    public void testUnionUnsortedStress() {
        final StoreTransaction txn = getStoreTransaction();
        final int count = 10000;
        for (int i = 0; i < count; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("number", i);
        }
        txn.flush();
        final int threshold = 28;
        EntityIterable result = EntityIterableBase.EMPTY;
        for (int i = 0; i < 200; ++i) {
            result = result.union(txn.find("Issue", "number", i, count - threshold));
        }
        Assert.assertEquals(count - threshold + 1, (int) result.size());
    }

    public void testMinus() {
        final StoreTransaction txn = getStoreTransaction();
        txn.flush();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("name", "Test issue #" + (i % 10));
            txn.newEntity("Comment");
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.getAll("Issue").minus(EntityIterableBase.EMPTY).size());
        Assert.assertEquals(0, (int) EntityIterableBase.EMPTY.minus(txn.getAll("Issue")).size());
        Assert.assertEquals(100, (int) txn.getAll("Issue").minus(txn.getAll("Comment")).size());
        Assert.assertEquals(90, (int) txn.getAll("Issue").minus(txn.find("Issue", "name", "Test issue #0")).size());
        Assert.assertEquals(90, (int) txn.getAll("Issue").minus(txn.find("Issue", "name", "Test issue #1")).size());
        Assert.assertEquals(90, (int) txn.getAll("Issue").minus(txn.find("Issue", "name", "Test issue #2")).size());
        Assert.assertEquals(90, (int) txn.getAll("Issue").minus(txn.find("Issue", "name", "Test issue #3")).size());
    }

    public void testSingularMinus() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("name", "Test issue #" + (i % 10));
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.getAll("Issue").minus(txn.getAll("Comment")).size());
        Assert.assertEquals(100, (int) txn.getAll("Issue").minus(txn.find("Issue", "name", "noname")).size());
    }

    public void testMinusUnsorted() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", 99 - i);
            txn.newEntity("Comment");
        }
        txn.flush();
        Assert.assertEquals(89, (int) txn.getAll("Issue").minus(txn.find("Issue", "size", 50, 60)).size());
        Assert.assertEquals(79, (int) txn.getAll("Issue").minus(txn.find("Issue", "size", 30, 50)).size());
        Assert.assertEquals(49, (int) txn.getAll("Issue").minus(txn.find("Issue", "size", 11, 61)).size());
        Assert.assertEquals(0, (int) txn.getAll("Issue").minus(txn.find("Issue", "size", 0, 100)).size());
    }

    public void testConcat() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("name", "Test issue #" + (i % 10));
            txn.newEntity("Comment");
        }
        txn.flush();
        Assert.assertEquals(100, (int) EntityIterableBase.EMPTY.concat(txn.getAll("Issue")).size());
        Assert.assertEquals(100, (int) txn.getAll("Issue").concat(EntityIterableBase.EMPTY).size());
        Assert.assertEquals(200, (int) txn.getAll("Issue").concat(txn.getAll("Issue")).size());
        Assert.assertEquals(200, (int) txn.getAll("Issue").concat(txn.getAll("Comment")).size());
        Assert.assertEquals(20, (int) txn.find("Issue", "name", "Test issue #0").concat(txn.find("Issue", "name", "Test issue #1")).size());
        Assert.assertEquals(30, (int) txn.find("Issue", "name", "Test issue #0").concat(txn.find("Issue", "name", "Test issue #1")).concat(txn.find("Issue", "name", "Test issue #2")).size());
    }

    @TestFor(issues = "XD-566")
    public void testConcat2() {
        final StoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue");
        Assert.assertEquals(1, toList(txn.getAll("User").concat(txn.getAll("Issue"))).size());
        Assert.assertTrue(txn.flush());
        txn.newEntity("User");
        Assert.assertTrue(txn.flush());
        Assert.assertEquals(2, toList(txn.getAll("User").concat(txn.getAll("Issue"))).size());
        txn.getAll("User").getFirst().delete();
        Assert.assertTrue(txn.flush());
        while (true) {
            txn.revert();
            final EntityIterableBase concat = (EntityIterableBase) txn.getAll("User").concat(txn.getAll("Issue"));
            Assert.assertEquals(1, toList(concat).size());
            if (concat.isCached()) break;
            Thread.yield();
        }
        txn.revert();
        txn.newEntity("User");
        Assert.assertTrue(txn.flush());
        final EntityIterableBase concat = (EntityIterableBase) txn.getAll("User").concat(txn.getAll("Issue"));
        Assert.assertFalse(concat.isCached());
        Assert.assertEquals(2, toList(concat).size());
    }

    private static List<Entity> toList(Iterable<Entity> it) {
        final List<Entity> result = new ArrayList<>();
        for (Entity entity : it) {
            result.add(entity);
        }
        return result;
    }
}
