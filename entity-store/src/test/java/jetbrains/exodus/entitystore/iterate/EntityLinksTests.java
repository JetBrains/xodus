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
import org.junit.Assert;

import java.util.Arrays;
import java.util.Iterator;

@SuppressWarnings(
        {"HardCodedStringLiteral", "AutoBoxing", "JUnitTestClassNamingConvention", "StringContatenationInLoop", "BusyWait"})
public class EntityLinksTests extends EntityStoreTestBase {

    private String debugLinkDataGetterValue;

    @Override
    public void setUp() throws Exception {
        debugLinkDataGetterValue = System.setProperty(PersistentEntityStoreConfig.DEBUG_LINK_DATA_GETTER, "true");
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (debugLinkDataGetterValue == null) {
            System.clearProperty(PersistentEntityStoreConfig.DEBUG_LINK_DATA_GETTER);
        } else {
            System.setProperty(PersistentEntityStoreConfig.DEBUG_LINK_DATA_GETTER, debugLinkDataGetterValue);
        }
        super.tearDown();
    }

    public void testAddAndIterateLinks() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        issue.setProperty("description", "Test issue");
        for (int i = 0; i < 10; ++i) {
            final Entity comment = txn.newEntity("Comment");
            comment.setProperty("body", "Comment" + i);
            issue.addLink("comment", comment);
            comment.addLink("issue", issue);
        }
        txn.flush();
        int i = 0;
        final Iterable<Entity> comments = issue.getLinks("comment");
        for (final Entity comment : comments) {
            Assert.assertEquals("Comment" + i, comment.getProperty("body"));
            final Iterator<Entity> iterator = comment.getLinks("issue").iterator();
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(issue, iterator.next());
            Assert.assertFalse(iterator.hasNext());
            ++i;
        }
        Assert.assertEquals(10, i);
    }

    public void testAddAndIterateReorder() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        final Entity[] issues = new Entity[10];
        for (int i = 0; i < issues.length; i++) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("description", "Test issue " + i);
            issues[i] = issue;
        }
        for (int i = 0; i < 10; ++i) {
            Entity issue = issues[9 - i];
            final Entity comment = txn.newEntity("Comment");
            comment.setProperty("body", "Comment" + i);
            issue.addLink("comment", comment);
            comment.addLink("issue", issue);
        }
        txn.flush();
        Assert.assertEquals(0, (int) txn.findWithLinks("Issue", "issue").size());
        Assert.assertEquals(10, (int) txn.findWithLinks("Issue", "comment").size());
    }

    public void testAddAndDeleteSomeLinks() throws Exception {
        testAddAndIterateLinks();
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue;
        Iterator<Entity> iterator = txn.getAll("Issue").iterator();
        Assert.assertTrue(iterator.hasNext());
        issue = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        // delete each odd link
        int i = 0;
        for (final Entity comment : issue.getLinks("comment")) {
            if ((++i & 1) != 0) {
                issue.deleteLink("comment", comment);
                comment.deleteLink("issue", issue);
            }
        }
        txn.flush();
        i = 0;
        for (final Entity comment : issue.getLinks("comment")) {
            ++i;
            final Iterable<Entity> parent = comment.getLinks("issue");
            iterator = parent.iterator();
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(issue, iterator.next());
            Assert.assertFalse(iterator.hasNext());
        }
        Assert.assertEquals(5, i);
    }

    public void testAddAndDeleteAllLinks() throws Exception {
        testAddAndIterateLinks();
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue;
        final EntityIterable issues = txn.getAll("Issue");
        final Iterator<Entity> iterator = issues.iterator();
        Assert.assertTrue(iterator.hasNext());
        issue = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        issue.deleteLinks("comment");
        txn.flush();
        Assert.assertFalse(issue.getLinks("comment").iterator().hasNext());
    }

    public void testFindLinks() throws Exception {
        testAddAndIterateLinks();
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue;
        final EntityIterable issues = txn.getAll("Issue");
        final Iterator<Entity> iterator = issues.iterator();
        Assert.assertTrue(iterator.hasNext());
        issue = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(10, (int) txn.findLinks("Comment", issue, "issue").size());
        for (final Entity comment : txn.findLinks("Comment", issue, "issue")) {
            Assert.assertEquals(0, (int) txn.findLinks("Comment", comment, "comment").size());
            Assert.assertEquals(1, (int) txn.findLinks("Issue", comment, "comment").size());
        }
    }

    public void testFindLinks2() throws Exception {
        testAddAndIterateLinks();
        final StoreTransaction txn = getStoreTransaction();
        Assert.assertEquals(txn.findLinks("Comment", txn.getAll("Issue"), "issue").size(),
                txn.getAll("Comment").size());
    }

    public void testSingleLink() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        txn.flush();
        Assert.assertNull(issue.getLink("comment"));
        final Entity comment = txn.newEntity("Comment");
        issue.addLink("comment", comment);
        txn.flush();
        Assert.assertEquals(comment, issue.getLink("comment"));
        issue.addLink("comment", txn.newEntity("Comment"));
        txn.flush();
        final boolean[] wereExceptions = {false};
        try {
            issue.getLink("comment");
        } catch (Exception e) {
            wereExceptions[0] = true;
        }
        Assert.assertTrue(wereExceptions[0]);
    }

    public void testGetSetLink() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        txn.flush();
        Assert.assertNull(issue.getLink("comment"));
        final Entity comment = txn.newEntity("Comment");
        issue.setLink("comment", comment);
        txn.flush();
        Assert.assertEquals(comment, issue.getLink("comment"));
        final Entity comment1 = txn.newEntity("Comment");
        issue.setLink("comment", comment1);
        txn.flush();
        Assert.assertFalse(comment.equals(issue.getLink("comment")));
        Assert.assertEquals(comment1, issue.getLink("comment"));
        final Entity comment2 = txn.newEntity("Comment");
        issue.setLink("comment", comment2);
        txn.flush();
        Assert.assertFalse(comment.equals(issue.getLink("comment")));
        Assert.assertFalse(comment1.equals(issue.getLink("comment")));
        Assert.assertEquals(comment2, issue.getLink("comment"));
    }

    public void testSetNullLinkSameAsDelete() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.newEntity("Issue");
        final Entity comment = txn.newEntity("Comment");
        issue.setLink("comment", comment);
        txn.flush();
        Assert.assertEquals(comment, issue.getLink("comment"));
        issue.setLink("comment", null);
        txn.flush();
        Assert.assertNull(issue.getLink("comment"));
    }

    public void testSetLink_CachedFindLinks() {
        final StoreTransaction txn = getStoreTransaction();
        Entity issue = txn.newEntity("Issue");
        txn.flush();
        Assert.assertNull(issue.getLink("comment"));
        Entity comment = txn.newEntity("Comment");
        Assert.assertEquals(0, (int) txn.findLinks("Issue", comment, "comment").size());
        txn.flush();
        Assert.assertEquals(0, (int) txn.findLinks("Issue", comment, "comment").size());
        issue.setLink("comment", comment);
        Assert.assertEquals(1, (int) txn.findLinks("Issue", comment, "comment").size());
        txn.flush();
        Assert.assertEquals(1, (int) txn.findLinks("Issue", comment, "comment").size());
        issue = txn.newEntity("Issue");
        issue.setLink("comment", comment);
        Assert.assertEquals(2, (int) txn.findLinks("Issue", comment, "comment").size());
        txn.flush();
        Assert.assertEquals(2, (int) txn.findLinks("Issue", comment, "comment").size());
        Entity oldComment = comment;
        comment = txn.newEntity("Comment");
        issue.setLink("comment", comment);
        Assert.assertEquals(1, (int) txn.findLinks("Issue", comment, "comment").size());
        txn.flush();
        Assert.assertEquals(1, (int) txn.findLinks("Issue", comment, "comment").size());
        comment = oldComment;
        issue.setLink("comment", comment);
        Assert.assertEquals(2, (int) txn.findLinks("Issue", comment, "comment").size());
        txn.flush();
        Assert.assertEquals(2, (int) txn.findLinks("Issue", comment, "comment").size());
    }

    public void testFindLinks3() throws Exception {
        testAddAndIterateLinks();
        final StoreTransaction txn = getStoreTransaction();
        final Entity issue = txn.getAll("Issue").iterator().next();
        Assert.assertNotNull(issue);
        final Entity state1 = txn.newEntity("State");
        state1.setProperty("name", "state1");
        txn.newEntity("Issue").setLink("state", state1);
        final Entity state2 = txn.newEntity("State");
        state2.setProperty("name", "state2");
        issue.setLink("state", state2);
        txn.flush();
        Assert.assertEquals("state1", state1.getProperty("name"));
        txn.findLinks("Issue", state1, "state");
        Assert.assertEquals("state2", state2.getProperty("name"));
        Assert.assertEquals(state2, txn.findLinks("Issue", state2, "state").iterator().next().getLink("state"));
        txn.newEntity("Issue").setLink("state", state1);
        txn.newEntity("Issue").setLink("state", state2);
        issue.setLink("state", state1);
        txn.flush();
        final EntityIterator it = txn.findLinks("Issue", state2, "state").union(txn.findLinks("Issue", state1, "state")).iterator();
        it.hasNext();
        final Entity issue1 = it.next();
        Assert.assertNotNull(issue1);
        Assert.assertEquals(issue, issue1);
        Assert.assertEquals(state1, issue1.getLink("state"));
    }

    public void testFindWithLinks() throws Exception {
        testAddAndIterateLinks();
        final StoreTransaction txn = getStoreTransaction();
        Assert.assertEquals(0, (int) txn.findWithLinks("Issue", "issue").size());
        Assert.assertEquals(10, (int) txn.findWithLinks("Issue", "comment").size());
        Assert.assertEquals(1, (int) txn.findWithLinks("Issue", "comment").distinct().size());
    }

    public void testFindWithLinks2() throws Exception {
        testAddAndIterateLinks();
        final StoreTransaction txn = getStoreTransaction();
        Assert.assertEquals(0, (int) txn.findWithLinks("Issue", "issue", "Comment", "comment").size());
        Assert.assertEquals(10, (int) txn.findWithLinks("Issue", "comment", "Comment", "issue").size());
        Assert.assertEquals(1, (int) txn.findWithLinks("Issue", "comment", "Comment", "issue").distinct().size());
    }

    public void testCreateFindLinks() {
        final StoreTransaction txn = getStoreTransaction();
        Entity e = txn.newEntity("Issue");
        Entity owner = txn.newEntity("User");
        e.addLink("owner", owner);
        txn.flush();
        for (int i = 1; i < 1000; ++i) {
            if (txn.findLinks("Issue", owner, "owner").indexOf(owner) < 0) {
                e = txn.newEntity("Issue");
                e.addLink("owner", owner);
                txn.flush();
            }
            final EntityIterable it = txn.findLinks("Issue", owner, "owner");
            if (it.indexOf(e) < 0) {
                throw new RuntimeException("0: Iteration " + i + ", it " + it.toString());
            }
        }
    }

    @TestFor(issues = "XD-517")
    public void testInvalidationOfToLinks() throws InterruptedException {
        final PersistentStoreTransaction txn = getStoreTransaction();
        PersistentEntity issue = txn.newEntity("Issue");
        PersistentEntity user = txn.newEntity("User");
        for (int i = 0; i < 10; ++i) {
            PersistentEntity event = txn.newEntity("Event");
            event.addLink("author", user);
            event.addLink("issue", issue);
        }
        txn.flush();
        Assert.assertEquals(10, txn.findLinks("Event", issue, "issue").size());
        Thread.sleep(1000);
        Assert.assertTrue(((EntityIteratorBase) txn.findLinks("Event", issue, "issue").iterator()).getIterable().isCachedInstance());
        for (int i = 0; i < 10; ++i) {
            PersistentEntity event = txn.newEntity("Event");
            event.addLink("author", user);
            event.addLink("issue", issue);
            txn.flush();
            Assert.assertEquals((11 + i), txn.findLinks("Event", issue, "issue").size());
        }
    }

    @TestFor(issues = "XD-518")
    public void testInvalidationFromSetLinks() throws InterruptedException {
        final PersistentStoreTransaction txn = getStoreTransaction();
        PersistentEntity issue = txn.newEntity("Issue");
        for (int i = 0; i < 10; ++i) {
            issue.addLink("related", txn.newEntity("Issue"));
            issue.addLink("subtask", txn.newEntity("Issue"));
            issue.addLink("link0", txn.newEntity("Issue"));
            issue.addLink("required for", txn.newEntity("Issue"));
            issue.addLink("duplicate", txn.newEntity("Issue"));
            issue.addLink("link1", txn.newEntity("Issue"));
            issue.addLink("is parent for", txn.newEntity("Issue"));
        }
        txn.flush();
        Assert.assertEquals(30, issue.getLinks(
                Arrays.asList("related", "duplicate", "is parent for")).size());
        Thread.sleep(1000);
        Assert.assertTrue(((EntityIteratorBase) issue.getLinks(
                Arrays.asList("related", "duplicate", "is parent for")).iterator()).getIterable().isCachedInstance());
        for (int i = 0; i < 10; ++i) {
            issue.addLink("duplicate", txn.newEntity("Issue"));
            txn.flush();
            Assert.assertEquals(31 + i, issue.getLinks(
                    Arrays.asList("related", "duplicate", "is parent for")).size());
        }
    }

    public void testInvalidationOfBinaryOperatorsWithFromLinks() throws InterruptedException {
        final PersistentStoreTransaction txn = getStoreTransaction();
        PersistentEntity issue = txn.newEntity("Issue");
        Entity owner = txn.newEntity("User");
        issue.addLink("owner", owner);
        owner.setProperty("admin", "admin");
        txn.flush();
        EntityIterable it;
        for (int i = 0; i < 8; ++i) {
            it = txn.find("User", "admin", "admin").intersect(issue.getLinks("owner"));
            Assert.assertEquals(1, (int) it.size());
            it = txn.find("User", "admin", "admin").union(issue.getLinks("owner"));
            Assert.assertEquals(1, (int) it.size());
            Thread.sleep(400);
        }
        owner = txn.newEntity("User");
        owner.setProperty("admin", "admin");
        issue.addLink("owner", owner);
        txn.flush();
        it = txn.find("User", "admin", "admin").intersect(issue.getLinks("owner"));
        Assert.assertEquals(2, (int) it.size());
        it = txn.find("User", "admin", "admin").union(issue.getLinks("owner"));
        Assert.assertEquals(2, (int) it.size());
        issue.deleteLink("owner", owner);
        txn.flush();
        it = txn.find("User", "admin", "admin").intersect(issue.getLinks("owner"));
        Assert.assertEquals(1, (int) it.size());
        it = txn.find("User", "admin", "admin").union(issue.getLinks("owner"));
        Assert.assertEquals(2, (int) it.size());
        issue.addLink("owner", owner);
        txn.flush();
        it = txn.find("User", "admin", "admin").intersect(issue.getLinks("owner"));
        Assert.assertEquals(2, (int) it.size());
        it = txn.find("User", "admin", "admin").union(issue.getLinks("owner"));
        Assert.assertEquals(2, (int) it.size());
    }

    public void testInvalidationOfBinaryOperatorsWithToLinks() throws InterruptedException {
        final StoreTransaction txn = getStoreTransaction();
        Entity issue = txn.newEntity("Issue");
        Entity owner = txn.newEntity("User");
        issue.addLink("owner", owner);
        issue.setProperty("summary", "test");
        txn.flush();
        EntityIterable it;
        for (int i = 0; i < 8; ++i) {
            it = txn.find("Issue", "summary", "test").intersect(txn.findLinks("Issue", owner, "owner"));
            Assert.assertEquals(1, (int) it.size());
            it = txn.find("Issue", "summary", "test").union(txn.findLinks("Issue", owner, "owner"));
            Assert.assertEquals(1, (int) it.size());
            Thread.sleep(400);
        }
        issue = txn.newEntity("Issue");
        issue.addLink("owner", owner);
        issue.setProperty("summary", "test");
        txn.flush();
        it = txn.find("Issue", "summary", "test").intersect(txn.findLinks("Issue", owner, "owner"));
        Assert.assertEquals(2, (int) it.size());
        it = txn.find("Issue", "summary", "test").union(txn.findLinks("Issue", owner, "owner"));
        Assert.assertEquals(2, (int) it.size());
        issue.delete();
        txn.flush();
        it = txn.find("Issue", "summary", "test");
        Assert.assertEquals(1, (int) it.size());
        it = txn.findLinks("Issue", owner, "owner");
        Assert.assertEquals(1, (int) it.size());
        it = txn.find("Issue", "summary", "test").intersect(txn.findLinks("Issue", owner, "owner"));
        Assert.assertEquals(1, (int) it.size());
        it = txn.find("Issue", "summary", "test").union(txn.findLinks("Issue", owner, "owner"));
        Assert.assertEquals(1, (int) it.size());
    }

    public void testInvalidationOfBinaryOperatorsWithToLinksCyclic() {
        final StoreTransaction txn = getStoreTransaction();
        Entity issue = txn.newEntity("Issue");
        Entity owner = txn.newEntity("User");
        issue.addLink("owner", owner);
        issue.setProperty("summary", "test");
        txn.flush();
        EntityIterable it;
        for (int i = 0; i < 1000; ++i) {
            it = txn.find("Issue", "summary", "test").intersect(txn.findLinks("Issue", owner, "owner"));
            Assert.assertEquals(1, (int) it.size());
            it = txn.find("Issue", "summary", "test").union(txn.findLinks("Issue", owner, "owner"));
            Assert.assertEquals(1, (int) it.size());
            issue = txn.newEntity("Issue");
            issue.addLink("owner", owner);
            issue.setProperty("summary", "test");
            txn.flush();
            it = txn.find("Issue", "summary", "test").intersect(txn.findLinks("Issue", owner, "owner"));
            Assert.assertEquals(2, (int) it.size());
            it = txn.find("Issue", "summary", "test").union(txn.findLinks("Issue", owner, "owner"));
            Assert.assertEquals(2, (int) it.size());
            issue.delete();
            txn.flush();
            if ((i % 100) == 0) {
                System.out.println("i = " + i);
            }
        }
        it = txn.find("Issue", "summary", "test");
        Assert.assertEquals(1, (int) it.size());
        it = txn.findLinks("Issue", owner, "owner");
        Assert.assertEquals(1, (int) it.size());
        it = txn.find("Issue", "summary", "test").intersect(txn.findLinks("Issue", owner, "owner"));
        Assert.assertEquals(1, (int) it.size());
        it = txn.find("Issue", "summary", "test").union(txn.findLinks("Issue", owner, "owner"));
        Assert.assertEquals(1, (int) it.size());
    }
}