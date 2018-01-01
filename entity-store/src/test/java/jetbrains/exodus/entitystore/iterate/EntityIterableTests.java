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
import jetbrains.exodus.entitystore.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.Iterator;

@SuppressWarnings({"HardCodedStringLiteral", "AutoBoxing", "ConstantConditions", "JUnitTestClassNamingConvention",
        "StringContatenationInLoop", "UnusedDeclaration", "WhileLoopReplaceableByForEach", "LoopStatementThatDoesntLoop"})
public class EntityIterableTests extends EntityStoreTestBase {

    protected String[] casesThatDontNeedExplicitTxn() {
        return new String[]{"testEntityIterableCacheIsInvalidatedOnStoreClear"};
    }

    public void testIterateAllEntities() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i);
            entity.setProperty("size", i);
        }
        txn.flush();
        int i = 0;
        for (final Entity entity : txn.getAll("Issue")) {
            Assert.assertEquals(i, entity.getProperty("size"));
            Assert.assertEquals("Test issue #" + i, entity.getProperty("description"));
            ++i;
        }
        Assert.assertEquals(100, i);
    }

    public void testNestedIterateAllEntities() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 20; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i);
            entity.setProperty("size", i);
        }
        txn.flush();
        int i = 0;
        final EntityIterable allIssues1 = txn.getAll("Issue");
        for (final Entity entity : allIssues1) {
            int j = 0;
            final EntityIterable allIssues2 = txn.getAll("Issue");
            for (final Entity entity1 : allIssues2) {
                Assert.assertEquals(j, entity1.getProperty("size"));
                Assert.assertEquals("Test issue #" + j, entity1.getProperty("description"));
                ++j;
            }
            Assert.assertEquals(i, entity.getProperty("size"));
            Assert.assertEquals("Test issue #" + i, entity.getProperty("description"));
            ++i;
        }
        Assert.assertEquals(20, i);
    }

    public void testNestedIterateAllEntities2() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 20; ++i) {
            final Entity entity = txn.newEntity("Issue");
            entity.setProperty("description", "Test issue #" + i);
            entity.setProperty("size", i);
        }
        txn.flush();
        int i = 0;
        final EntityIterable allIssues = txn.getAll("Issue");
        for (final Entity entity : allIssues) {
            int j = 0;
            for (final Entity entity1 : allIssues) {
                Assert.assertEquals(j, entity1.getProperty("size"));
                Assert.assertEquals("Test issue #" + j, entity1.getProperty("description"));
                ++j;
            }
            Assert.assertEquals(i, entity.getProperty("size"));
            Assert.assertEquals("Test issue #" + i, entity.getProperty("description"));
            ++i;
        }
        Assert.assertEquals(20, i);
    }

    public void testSingularGetAll() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity entity = txn.newEntity("Issue");
        entity.setProperty("name", "noname");
        txn.flush();
        Assert.assertEquals(0, (int) txn.getAll("Comment").size());
    }

    public void testMultipleIterators() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", 99 - i);
            txn.newEntity("Comment");
        }
        txn.flush();
        final EntityIterable allIssues = txn.getAll("Issue");
        Iterator<Entity> iterator = allIssues.iterator();
        while (iterator.hasNext()) {
            Assert.assertEquals("Issue", iterator.next().getType());
            break;
        }
        iterator = allIssues.iterator();
        while (iterator.hasNext()) {
            Assert.assertEquals("Issue", iterator.next().getType());
            break;
        }
    }

    public void testGetAllCount() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(100, (int) txn.getAll("Issue").size());
        Assert.assertEquals(100, (int) txn.getAll("Issue").size());
    }

    public void testLinksCount() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity issue = txn.newEntity("Issue");
        for (int i = 0; i < 100; ++i) {
            issue.addLink("comment", txn.newEntity("Comment"));
        }
        txn.flush();
        Assert.assertEquals(100, (int) issue.getLinks("comment").size());
    }

    public void testIdRange() {
        getEntityStore().getConfig().setCachingDisabled(false);
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", (i & 1) == 0 ? 50 : 100);
        }
        txn.flush();
        EntityIterable issues = txn.findIds("Issue", 10, 20);
        EntityIterable issues2 = txn.findIds("Issue", 16, 73);
        assertEquals(11, issues.size());
        assertEquals(58, issues2.size());
        checkIdRange(issues, 10, 20);
        checkIdRange(issues2, 16, 73);
        issues = txn.findIds("Issue", 10, 20);
        issues2 = txn.findIds("Issue", 16, 73);
        assertTrue(((EntityIteratorBase) issues.iterator()).getIterable().isCachedInstance());
        assertTrue(((EntityIteratorBase) issues2.iterator()).getIterable().isCachedInstance());
        checkIdRange(issues, 10, 20);
        checkIdRange(issues2, 16, 73);
    }

    public void testFindByPropCount() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", (i & 1) == 0 ? 50 : 100);
        }
        txn.flush();
        Assert.assertEquals(50, (int) txn.find("Issue", "size", 50).size());
        Assert.assertEquals(50, (int) txn.find("Issue", "size", 50).size());
        Assert.assertEquals(50, (int) txn.find("Issue", "size", 100).size());
        Assert.assertEquals(50, (int) txn.find("Issue", "size", 100).size());
        Assert.assertEquals(0, (int) txn.find("Issue", "size", 101).size());
    }

    public void testFindByRangeCount() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("size", i / 10);
        }
        txn.flush();
        Assert.assertEquals(30, (int) txn.find("Issue", "size", 0, 2).size());
        Assert.assertEquals(60, (int) txn.find("Issue", "size", 1, 6).size());
        Assert.assertEquals(0, (int) txn.find("Issue", "size", 10, 20).size());
    }

    public void testBinaryOperatorAppliedToEmptyIterable() {
        final StoreTransaction txn = getStoreTransaction();
        final EntityIterable no_issues = txn.getAll("Issue");
        Assert.assertEquals(0, (int) no_issues.size());
        Assert.assertEquals(0, (int) no_issues.intersect(txn.getAll("Comment")).size());
        Assert.assertEquals(0, (int) no_issues.union(txn.getAll("Comment")).size());
        Assert.assertEquals(0, (int) no_issues.minus(txn.getAll("Comment")).size());
    }

    public void testSkipIterator() {
        Assert.assertFalse(EntityIterableBase.EMPTY.iterator().skip(1));
        Assert.assertFalse(EntityIterableBase.EMPTY.iterator().skip(0));
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        for (int i = 0; i < 100; ++i) {
            issue.addLink("comment", txn.newEntity("Comment"));
        }
        txn.flush();
        Assert.assertFalse(txn.getAll("Issue").iterator().skip(2));
        Assert.assertFalse(txn.getAll("Issue").iterator().skip(1));
        Assert.assertTrue(txn.getAll("Issue").iterator().skip(0));
        Assert.assertFalse(txn.getAll("Comment").iterator().skip(100));
        Assert.assertTrue(txn.getAll("Comment").iterator().skip(99));
        Assert.assertTrue(txn.getAll("Comment").union(txn.getAll("Issue")).iterator().skip(100));
        Assert.assertFalse(txn.getAll("Comment").union(txn.getAll("Issue")).iterator().skip(101));
        Assert.assertFalse(txn.getAll("Comment").intersect(txn.getAll("Issue")).iterator().skip(0));
    }

    public void testSkipIterable() {
        Assert.assertTrue(EntityIterableBase.EMPTY == EntityIterableBase.EMPTY.skip(0));
        Assert.assertTrue(EntityIterableBase.EMPTY == EntityIterableBase.EMPTY.skip(1));
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(80, (int) txn.getAll("Issue").skip(20).size());
        Assert.assertEquals(60, (int) txn.getAll("Issue").skip(20).skip(20).size());
        Assert.assertEquals(40, (int) txn.getAll("Issue").skip(20).skip(20).skip(20).size());
        Assert.assertEquals(20, (int) txn.getAll("Issue").skip(20).skip(20).skip(20).skip(20).size());
        Assert.assertEquals(0, (int) txn.getAll("Issue").skip(20).skip(20).skip(20).skip(20).skip(20).size());
        Assert.assertEquals(0, (int) txn.getAll("Issue").skip(20).skip(20).skip(20).skip(20).skip(21).size());
    }

    public void testTakeIterable() {
        Assert.assertTrue(EntityIterableBase.EMPTY == EntityIterableBase.EMPTY.take(0));
        Assert.assertTrue(EntityIterableBase.EMPTY == EntityIterableBase.EMPTY.take(1));
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 100; ++i) {
            txn.newEntity("Issue");
        }
        txn.flush();
        Assert.assertEquals(80, (int) txn.getAll("Issue").take(80).size());
        Assert.assertEquals(60, (int) txn.getAll("Issue").take(80).take(60).size());
        Assert.assertEquals(40, (int) txn.getAll("Issue").take(80).take(60).take(40).size());
        Assert.assertEquals(20, (int) txn.getAll("Issue").take(20).take(40).take(60).take(80).size());
        Assert.assertEquals(40, (int) txn.getAll("Issue").take(40).take(60).take(80).size());
        Assert.assertEquals(60, (int) txn.getAll("Issue").take(60).take(80).size());
        Assert.assertEquals(0, (int) txn.getAll("Issue").take(60).take(0).size());
        Assert.assertEquals(0, (int) txn.getAll("Issue").take(0).take(60).size());
    }

    public void testSelectDistinct() {
        final StoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"));
        final Entity user = txn.newEntity("User");
        txn.newEntity("Issue").addLink("assignee", user);
        txn.newEntity("Issue").addLink("assignee", user);
        txn.flush();
        Assert.assertEquals(2, (int) txn.getAll("Issue").selectDistinct("assignee").size());
    }

    public void testSelectDistinct2() {
        final StoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"));
        final Entity user = txn.newEntity("User");
        txn.newEntity("Issue").addLink("assignee", user);
        txn.newEntity("Issue").addLink("assignee", user);
        txn.newEntity("Issue");
        txn.flush();
        Assert.assertEquals(3, (int) txn.getAll("Issue").selectDistinct("assignee").size());
    }

    public void testSelectDistinctFromEmptySequence() {
        final StoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue");
        txn.newEntity("User");
        txn.flush();
        Assert.assertEquals(0, (int) txn.getAll("Issue").intersect(txn.getAll("User")).selectDistinct("unknown_link").size());
    }

    public void testSelectDistinctSingular() {
        final StoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue");
        txn.flush();
        Assert.assertEquals(1, (int) txn.getAll("Issue").size());
        Assert.assertEquals(1, (int) txn.getAll("Issue").selectDistinct("assignee").size());
        final Entity user = txn.newEntity("User");
        txn.newEntity("Issue").addLink("assignee", user);
        txn.newEntity("Issue").addLink("assignee", user);
        txn.flush();
        Assert.assertEquals(3, (int) txn.getAll("Issue").size());
        Assert.assertEquals(2, (int) txn.getAll("Issue").selectDistinct("assignee").size());
    }

    public void testSelectManyDistinct() {
        final StoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 10);
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"));
        final Entity user = txn.newEntity("User");
        txn.newEntity("Issue").addLink("assignee", user);
        txn.newEntity("Issue").addLink("assignee", user);
        txn.flush();
        Assert.assertEquals(2, (int) txn.getAll("Issue").selectManyDistinct("assignee").size());
    }

    public void testSelectManyDistinct2() {
        final StoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 10);
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"));
        final Entity user1 = txn.newEntity("User");
        final Entity user2 = txn.newEntity("User");
        final Entity issue1 = txn.newEntity("Issue");
        issue1.addLink("assignee", user1);
        issue1.addLink("assignee", user2);
        issue1.addLink("assignee", txn.newEntity("User"));
        final Entity issue2 = txn.newEntity("Issue");
        issue2.addLink("assignee", user1);
        issue2.addLink("assignee", user2);
        txn.flush();
        Assert.assertEquals(4, (int) txn.getAll("Issue").selectManyDistinct("assignee").size());
    }

    public void testSelectManyDistinctFromEmptySequence() {
        final StoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue");
        txn.newEntity("User");
        txn.flush();
        Assert.assertEquals(0, (int) txn.getAll("Issue").intersect(txn.getAll("User")).selectManyDistinct("unknown_link").size());
    }

    public void testSelectManyDistinct3() {
        final StoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 10);
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"));
        final Entity user1 = txn.newEntity("User");
        final Entity user2 = txn.newEntity("User");
        final Entity issue1 = txn.newEntity("Issue");
        issue1.addLink("assignee", user1);
        issue1.addLink("assignee", user2);
        issue1.addLink("assignee", txn.newEntity("User"));
        final Entity issue2 = txn.newEntity("Issue");
        issue2.addLink("assignee", user1);
        issue2.addLink("assignee", user2);
        txn.newEntity("Issue");
        txn.flush();
        Assert.assertEquals(5, (int) txn.getAll("Issue").selectManyDistinct("assignee").size());
    }

    public void testSelectManyDistinctSingular() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 10);
        txn.newEntity("Issue");
        txn.flush();
        Assert.assertEquals(0, (int) txn.getAll("Issue").selectManyDistinct("assignee").size());
        getEntityStore().getLinkId(txn, "assignee", true);
        txn.flush();
        Assert.assertEquals(1, (int) txn.getAll("Issue").selectManyDistinct("assignee").size());
        final Entity user = txn.newEntity("User");
        txn.newEntity("Issue").addLink("assignee", user);
        txn.newEntity("Issue").addLink("assignee", user);
        txn.flush();
        Assert.assertEquals(2, (int) txn.getAll("Issue").selectManyDistinct("assignee").size());
    }

    public void testFindLinks() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 10);
        txn.flush();
        for (int i = 0; i < 10; ++i) {
            final PersistentEntity issue = txn.newEntity("Issue");
            issue.addLink("author", txn.find("User", "login", "user" + i).getFirst());
        }
        txn.flush();
        final EntityIterable someUsers = txn.find("User", "login", "user3", "user8");
        final EntityIterable authoredIssues = txn.findWithLinks("Issue", "author");
        final EntityIterable links0 = txn.findLinks("Issue", someUsers, "author");
        Assert.assertEquals(someUsers.size(), links0.size());
        final EntityIterable links1 = ((EntityIterableBase) authoredIssues).findLinks(someUsers, "author");
        Assert.assertEquals(someUsers.size(), links1.size());
        final EntityIterator it0 = links0.iterator();
        final EntityIterator it1 = links1.iterator();
        while (it0.hasNext() || it1.hasNext()) {
            Assert.assertTrue(it0.hasNext());
            Assert.assertTrue(it1.hasNext());
            Assert.assertEquals(it0.nextId(), it1.nextId());
        }
    }

    public void testFindLinksSingular() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 1);
        final PersistentEntity issue = txn.newEntity("Issue");
        issue.addLink("author", txn.find("User", "login", "user0").getFirst());
        txn.flush();
        Assert.assertEquals(0L, EntityIterableBase.EMPTY.findLinks(txn.getAll("User"), "author").size());
        Assert.assertEquals(0L, ((EntityIterableBase) txn.getAll("Issue")).findLinks(EntityIterableBase.EMPTY, "author").size());
    }

    public void testGetFirst() {
        for (int i = 0; i < 256; ++i) {
            Assert.assertNull(EntityIterableBase.EMPTY.getFirst());
        }
        final PersistentStoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 10);
        txn.flush();
        Assert.assertNotNull(txn.getAll("User").getFirst());
        Assert.assertEquals("user0", txn.getAll("User").getFirst().getProperty("login"));
    }

    public void testGetLast() {
        for (int i = 0; i < 256; ++i) {
            Assert.assertNull(EntityIterableBase.EMPTY.getLast());
        }
        final PersistentStoreTransaction txn = getStoreTransaction();
        createNUsers(txn, 10);
        txn.flush();
        Assert.assertNotNull(txn.getAll("User").getLast());
        Assert.assertEquals("user9", txn.getAll("User").getLast().getProperty("login"));
    }

    public void testGetLastOfGetAll() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final int count = 100000;
        createNUsers(txn, count);
        txn.flush();
        final long started = System.currentTimeMillis();
        final Entity lastUser = txn.getAll("User").getLast();
        System.out.println(System.currentTimeMillis() - started);
        Assert.assertNotNull(lastUser);
        Assert.assertEquals("user" + (count - 1), lastUser.getProperty("login"));
    }

    public void testSingleEntityIterable_XD_408() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final int count = 1;
        createNUsers(txn, count);
        txn.flush();
        EntityIterable users = txn.getSingletonIterable(txn.getAll("User").getFirst());
        users = users.union(users);
        Assert.assertFalse(((EntityIterableBase) users).canBeCached());
        Assert.assertEquals(1L, users.getRoughSize());
    }

    public void testSingleEntityIterable2() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final int count = 1;
        createNUsers(txn, count);
        txn.flush();
        EntityIterable users = txn.getSingletonIterable(txn.getAll("User").getFirst());
        users = users.union(users);
        Assert.assertFalse(((EntityIterableBase) users).canBeCached());
        Assert.assertEquals(-1L, users.getRoughCount());
        getEntityStore().getAsyncProcessor().waitForJobs(100);
        Assert.assertEquals(1L, users.getRoughCount());
    }

    @TestFor(issues = "XD-502")
    public void testFindWithPropSortedCount() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        Assert.assertEquals(0, txn.findWithPropSortedByValue("User", "login").countImpl(txn));
        final int count = 10;
        createNUsers(txn, count);
        txn.flush();
        Assert.assertEquals(count, txn.findWithPropSortedByValue("User", "login").countImpl(txn));
    }

    public void testCachedInstanceIsEmpty() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final int count = 10;
        createNUsers(txn, count);
        txn.flush();
        final PersistentEntityStoreImpl store = getEntityStore();
        Assert.assertEquals(0, txn.getAll("User").indexOf(new PersistentEntity(store, new PersistentEntityId(0, 0))));
        final EntityIterableBase cachedInstance =
                store.getEntityIterableCache().putIfNotCached((EntityIterableBase) txn.getAll("User"));
        Assert.assertFalse(cachedInstance.isEmpty());
    }

    @TestFor(issues = "XD-522")
    public void testRoughSize() throws InterruptedException {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final int count = 10;
        createNUsers(txn, count);
        txn.flush();
        final EntityIterable allUsers = txn.findStartingWith("User", "login", "u");
        Assert.assertEquals(10L, allUsers.getRoughSize());
        Thread.sleep(1000);
        for (int i = 0; i < 3; ++i) {
            createNUsers(txn, 1);
            txn.flush();
            Assert.assertEquals(10L + i, allUsers.getRoughSize());
            Thread.sleep(1000);
        }
    }

    @TestFor(issues = "XD-536")
    public void testEntityIterableCacheIsInvalidatedOnStoreClear() {
        final PersistentEntityStoreImpl entityStore = getEntityStore();
        entityStore.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                createNUsers(txn, 10);
            }
        });
        entityStore.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                Assert.assertEquals(9, txn.getAll("User").indexOf(new PersistentEntity(entityStore, new PersistentEntityId(0, 9))));
            }
        });
        entityStore.clear();
        entityStore.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                createNUsers(txn, 1);
            }
        });
        entityStore.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                Assert.assertEquals(-1, txn.getAll("User").indexOf(new PersistentEntity(entityStore, new PersistentEntityId(0, 9))));
            }
        });
    }

    /**
     * Should fail with OOME being run in JVM with Xmx256m without fix of XD-458
     */
    public void test_XD_458() throws InterruptedException {
        final PersistentStoreTransaction txn = getStoreTransaction();
        getEntityStore().getConfig().setEntityIterableCacheCachingTimeout(10000000L);
        getEntityStore().getConfig().setEntityIterableCacheMaxSizeOfDirectValue(Integer.MAX_VALUE);
        System.out.println("Xmx = " + Runtime.getRuntime().maxMemory());
        final int startingCount = 2000000;
        for (int i = 0; i < startingCount; ++i) {
            txn.newEntity("User");
            if (i % 10000 == 0) {
                txn.flush();
            }
        }
        txn.flush();
        System.out.println(startingCount + " users created.");
        while (!getEntityStore().getEntityIterableCache().putIfNotCached((EntityIterableBase) txn.getAll("User")).isCachedInstance()) {
            Thread.sleep(1000);
        }
        System.out.println("getAll(\"User\") cached.");
        for (int i = 0; i < 80000; ++i) {
            Assert.assertEquals(i + startingCount, (int) txn.getAll("User").size());
            txn.newEntity("User");
            txn.flush();
        }
    }

    private static void checkIdRange(EntityIterable issues, int lo, int hi) {
        int i = lo;
        for (Entity e : issues) {
            assertEquals(i, e.getId().getLocalId());
            assertEquals((i & 1) == 0 ? 50 : 100, e.getProperty("size"));
            i++;
        }
        assertEquals(hi + 1, i);
    }

    private static void createNUsers(final StoreTransaction txn, final int n) {
        for (int i = 0; i < n; ++i) {
            final Entity user = txn.newEntity("User");
            user.setProperty("login", "user" + i);
        }
    }
}