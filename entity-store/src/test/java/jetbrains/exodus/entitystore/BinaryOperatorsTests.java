/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase;
import org.junit.Assert;

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
        final PersistentEntityStoreImpl store = getEntityStore();
        store.setCachingEnabled(true);
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("ready", true);
        }
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.getAll("Issue").intersect(txn.find("Issue", "ready", Boolean.TRUE)).size());
        Assert.assertTrue(((EntityIteratorBase) txn.getAll("Issue").intersect(
                txn.find("Issue", "ready", Boolean.TRUE)).iterator()).getIterable().isCachedWrapper());
        // commutative intersect will be cached as well
        Assert.assertTrue(((EntityIteratorBase) txn.find("Issue", "ready", Boolean.TRUE).intersect(
                txn.getAll("Issue")).iterator()).getIterable().isCachedWrapper());
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
        final PersistentEntityStoreImpl store = getEntityStore();
        store.setCachingEnabled(true);
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("ready", true);
        }
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(200, (int) txn.getAll("Issue").union(txn.find("Issue", "ready", Boolean.TRUE)).size());
        Assert.assertTrue(((EntityIteratorBase) txn.getAll("Issue").union(
                txn.find("Issue", "ready", Boolean.TRUE)).iterator()).getIterable().isCachedWrapper());
        // commutative union will be cached as well
        Assert.assertTrue(((EntityIteratorBase) txn.find("Issue", "ready", Boolean.TRUE).union(
                txn.getAll("Issue")).iterator()).getIterable().isCachedWrapper());
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
}
