/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.TestFor
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.kotlin.notNull
import org.junit.Assert

class EntityIterableTests : EntityStoreTestBase() {

    override fun casesThatDontNeedExplicitTxn(): Array<String> {
        return arrayOf("testEntityIterableCacheIsInvalidatedOnStoreClear")
    }

    fun testIterateAllEntities() {
        val txn = storeTransaction
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #$i")
            entity.setProperty("size", i)
        }
        txn.flush()
        var i = 0
        for (entity in txn.getAll("Issue")) {
            Assert.assertEquals(i, entity.getProperty("size"))
            Assert.assertEquals("Test issue #$i", entity.getProperty("description"))
            ++i
        }
        Assert.assertEquals(100, i.toLong())
    }

    fun testNestedIterateAllEntities() {
        val txn = storeTransaction
        for (i in 0..19) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #$i")
            entity.setProperty("size", i)
        }
        txn.flush()
        var i = 0
        val allIssues1 = txn.getAll("Issue")
        for (entity in allIssues1) {
            val allIssues2 = txn.getAll("Issue")
            for ((j, entity1) in allIssues2.withIndex()) {
                Assert.assertEquals(j, entity1.getProperty("size"))
                Assert.assertEquals("Test issue #$j", entity1.getProperty("description"))
            }
            Assert.assertEquals(i, entity.getProperty("size"))
            Assert.assertEquals("Test issue #$i", entity.getProperty("description"))
            ++i
        }
        Assert.assertEquals(20, i.toLong())
    }

    fun testNestedIterateAllEntities2() {
        val txn = storeTransaction
        for (i in 0..19) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #$i")
            entity.setProperty("size", i)
        }
        txn.flush()
        var i = 0
        val allIssues = txn.getAll("Issue")
        for (entity in allIssues) {
            for ((j, entity1) in allIssues.withIndex()) {
                Assert.assertEquals(j, entity1.getProperty("size"))
                Assert.assertEquals("Test issue #$j", entity1.getProperty("description"))
            }
            Assert.assertEquals(i, entity.getProperty("size"))
            Assert.assertEquals("Test issue #$i", entity.getProperty("description"))
            ++i
        }
        Assert.assertEquals(20, i.toLong())
    }

    fun testSingularGetAll() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        entity.setProperty("name", "noname")
        txn.flush()
        Assert.assertEquals(0, txn.getAll("Comment").size())
    }

    fun testMultipleIterators() {
        val txn = storeTransaction
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", 99 - i)
            txn.newEntity("Comment")
        }
        txn.flush()
        val allIssues = txn.getAll("Issue")
        var iterator: Iterator<Entity> = allIssues.iterator()
        while (iterator.hasNext()) {
            Assert.assertEquals("Issue", iterator.next().type)
            break
        }
        iterator = allIssues.iterator()
        while (iterator.hasNext()) {
            Assert.assertEquals("Issue", iterator.next().type)
            break
        }
    }

    fun testGetAllCount() {
        val txn = storeTransaction
        for (i in 0..99) {
            txn.newEntity("Issue")
        }
        txn.flush()
        Assert.assertEquals(100, txn.getAll("Issue").size())
        Assert.assertEquals(100, txn.getAll("Issue").size())
    }

    fun testLinksCount() {
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        for (i in 0..99) {
            issue.addLink("comment", txn.newEntity("Comment"))
        }
        txn.flush()
        Assert.assertEquals(100, issue.getLinks("comment").size())
    }

    fun testIdRange() {
        entityStore.config.isCachingDisabled = false
        val txn = storeTransaction
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", if (i and 1 == 0) 50 else 100)
        }
        txn.flush()
        var issues = txn.findIds("Issue", 10, 20)
        var issues2 = txn.findIds("Issue", 16, 73)
        assertEquals(11, issues.size())
        assertEquals(58, issues2.size())
        checkIdRange(issues, 10, 20)
        checkIdRange(issues2, 16, 73)
        issues = txn.findIds("Issue", 10, 20)
        issues2 = txn.findIds("Issue", 16, 73)
        assertTrue((issues.iterator() as EntityIteratorBase).iterable.isCachedInstance)
        assertTrue((issues2.iterator() as EntityIteratorBase).iterable.isCachedInstance)
        checkIdRange(issues, 10, 20)
        checkIdRange(issues2, 16, 73)
    }

    fun testFindByPropCount() {
        val txn = storeTransaction
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", if (i and 1 == 0) 50 else 100)
        }
        txn.flush()
        Assert.assertEquals(50, txn.find("Issue", "size", 50).size())
        Assert.assertEquals(50, txn.find("Issue", "size", 50).size())
        Assert.assertEquals(50, txn.find("Issue", "size", 100).size())
        Assert.assertEquals(50, txn.find("Issue", "size", 100).size())
        Assert.assertEquals(0, txn.find("Issue", "size", 101).size())
    }

    fun testFindByRangeCount() {
        val txn = storeTransaction
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("size", i / 10)
        }
        txn.flush()
        Assert.assertEquals(30, txn.find("Issue", "size", 0, 2).size())
        Assert.assertEquals(60, txn.find("Issue", "size", 1, 6).size())
        Assert.assertEquals(0, txn.find("Issue", "size", 10, 20).size())
    }

    fun testBinaryOperatorAppliedToEmptyIterable() {
        val txn = storeTransaction
        val noIssues = txn.getAll("Issue")
        Assert.assertEquals(0, noIssues.size())
        Assert.assertEquals(0, noIssues.intersect(txn.getAll("Comment")).size())
        Assert.assertEquals(0, noIssues.union(txn.getAll("Comment")).size())
        Assert.assertEquals(0, noIssues.minus(txn.getAll("Comment")).size())
    }

    fun testSkipIterator() {
        Assert.assertFalse(EntityIterableBase.EMPTY.iterator().skip(1))
        Assert.assertFalse(EntityIterableBase.EMPTY.iterator().skip(0))
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        for (i in 0..99) {
            issue.addLink("comment", txn.newEntity("Comment"))
        }
        txn.flush()
        Assert.assertFalse(txn.getAll("Issue").iterator().skip(2))
        Assert.assertFalse(txn.getAll("Issue").iterator().skip(1))
        Assert.assertTrue(txn.getAll("Issue").iterator().skip(0))
        Assert.assertFalse(txn.getAll("Comment").iterator().skip(100))
        Assert.assertTrue(txn.getAll("Comment").iterator().skip(99))
        Assert.assertTrue(txn.getAll("Comment").union(txn.getAll("Issue")).iterator().skip(100))
        Assert.assertFalse(txn.getAll("Comment").union(txn.getAll("Issue")).iterator().skip(101))
        Assert.assertFalse(txn.getAll("Comment").intersect(txn.getAll("Issue")).iterator().skip(0))
    }

    fun testSkipIterable() {
        Assert.assertTrue(EntityIterableBase.EMPTY === EntityIterableBase.EMPTY.skip(0))
        Assert.assertTrue(EntityIterableBase.EMPTY === EntityIterableBase.EMPTY.skip(1))
        val txn = storeTransaction
        for (i in 0..99) {
            txn.newEntity("Issue")
        }
        txn.flush()
        Assert.assertEquals(80, txn.getAll("Issue").skip(20).size())
        Assert.assertEquals(60, txn.getAll("Issue").skip(20).skip(20).size())
        Assert.assertEquals(40, txn.getAll("Issue").skip(20).skip(20).skip(20).size())
        Assert.assertEquals(20, txn.getAll("Issue").skip(20).skip(20).skip(20).skip(20).size())
        Assert.assertEquals(0, txn.getAll("Issue").skip(20).skip(20).skip(20).skip(20).skip(20).size())
        Assert.assertEquals(0, txn.getAll("Issue").skip(20).skip(20).skip(20).skip(20).skip(21).size())
    }

    fun testTakeIterable() {
        Assert.assertTrue(EntityIterableBase.EMPTY === EntityIterableBase.EMPTY.take(0))
        Assert.assertTrue(EntityIterableBase.EMPTY === EntityIterableBase.EMPTY.take(1))
        val txn = storeTransaction
        for (i in 0..99) {
            txn.newEntity("Issue")
        }
        txn.flush()
        Assert.assertEquals(80, txn.getAll("Issue").take(80).size())
        Assert.assertEquals(60, txn.getAll("Issue").take(80).take(60).size())
        Assert.assertEquals(40, txn.getAll("Issue").take(80).take(60).take(40).size())
        Assert.assertEquals(20, txn.getAll("Issue").take(20).take(40).take(60).take(80).size())
        Assert.assertEquals(40, txn.getAll("Issue").take(40).take(60).take(80).size())
        Assert.assertEquals(60, txn.getAll("Issue").take(60).take(80).size())
        Assert.assertEquals(0, txn.getAll("Issue").take(60).take(0).size())
        Assert.assertEquals(0, txn.getAll("Issue").take(0).take(60).size())
    }

    fun testSelectDistinct() {
        val txn = storeTransaction
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user = txn.newEntity("User")
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue").addLink("assignee", user)
        txn.flush()
        Assert.assertEquals(2, txn.getAll("Issue").selectDistinct("assignee").size())
    }

    fun testSelectDistinct2() {
        val txn = storeTransaction
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user = txn.newEntity("User")
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue")
        txn.flush()
        Assert.assertEquals(3, txn.getAll("Issue").selectDistinct("assignee").size())
    }

    fun testSelectDistinctSource() {
        val txn = storeTransaction
        repeat(10) { i ->
            txn.newEntity("Issue").apply {
                setProperty("i", i)
                addLink("assignee", txn.newEntity("User"))
            }
        }
        txn.flush()
        val iterator = txn.getAll("Issue").selectDistinct("assignee").iterator() as SourceMappingIterator
        var i = 0
        iterator.forEach { _ ->
            Assert.assertEquals(i++, txn.getEntity(iterator.sourceId).getProperty("i"))
        }
    }

    fun testSelectDistinctFromEmptySequence() {
        val txn = storeTransaction
        txn.newEntity("Issue")
        txn.newEntity("User")
        txn.flush()
        Assert.assertEquals(0, txn.getAll("Issue").intersect(txn.getAll("User")).selectDistinct("unknown_link").size())
    }

    fun testSelectDistinctSingular() {
        val txn = storeTransaction
        txn.newEntity("Issue")
        txn.flush()
        Assert.assertEquals(1, txn.getAll("Issue").size())
        Assert.assertEquals(1, txn.getAll("Issue").selectDistinct("assignee").size())
        val user = txn.newEntity("User")
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue").addLink("assignee", user)
        txn.flush()
        Assert.assertEquals(3, txn.getAll("Issue").size())
        Assert.assertEquals(2, txn.getAll("Issue").selectDistinct("assignee").size())
    }

    fun testSelectManyDistinct() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user = txn.newEntity("User")
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue").addLink("assignee", user)
        txn.flush()
        Assert.assertEquals(2, txn.getAll("Issue").selectManyDistinct("assignee").size())
    }

    fun testSelectManyDistinct2() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user1 = txn.newEntity("User")
        val user2 = txn.newEntity("User")
        val issue1 = txn.newEntity("Issue")
        issue1.addLink("assignee", user1)
        issue1.addLink("assignee", user2)
        issue1.addLink("assignee", txn.newEntity("User"))
        val issue2 = txn.newEntity("Issue")
        issue2.addLink("assignee", user1)
        issue2.addLink("assignee", user2)
        txn.flush()
        Assert.assertEquals(4, txn.getAll("Issue").selectManyDistinct("assignee").size())
    }

    fun testSelectManyDistinctFromEmptySequence() {
        val txn = storeTransaction
        txn.newEntity("Issue")
        txn.newEntity("User")
        txn.flush()
        Assert.assertEquals(0, txn.getAll("Issue").intersect(txn.getAll("User")).selectManyDistinct("unknown_link").size())
    }

    fun testSelectManyDistinct3() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user1 = txn.newEntity("User")
        val user2 = txn.newEntity("User")
        val issue1 = txn.newEntity("Issue")
        issue1.addLink("assignee", user1)
        issue1.addLink("assignee", user2)
        issue1.addLink("assignee", txn.newEntity("User"))
        val issue2 = txn.newEntity("Issue")
        issue2.addLink("assignee", user1)
        issue2.addLink("assignee", user2)
        txn.newEntity("Issue")
        txn.flush()
        Assert.assertEquals(5, txn.getAll("Issue").selectManyDistinct("assignee").size())
    }

    fun testSelectManyDistinctSource() {
        val txn = storeTransaction
        repeat(10) { i ->
            txn.newEntity("Issue").apply {
                setProperty("i", i)
                addLink("assignee", txn.newEntity("User"))
                addLink("assignee", txn.newEntity("User"))
            }
        }
        txn.flush()
        val iterator = txn.getAll("Issue").selectManyDistinct("assignee").iterator() as SourceMappingIterator
        var i = 0
        iterator.forEach { _ ->
            Assert.assertEquals(i++ / 2, txn.getEntity(iterator.sourceId).getProperty("i"))
        }
    }

    fun testSelectManyDistinctSingular() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue")
        txn.flush()
        Assert.assertEquals(0, txn.getAll("Issue").selectManyDistinct("assignee").size())
        entityStore.getLinkId(txn, "assignee", true)
        txn.flush()
        Assert.assertEquals(1, txn.getAll("Issue").selectManyDistinct("assignee").size())
        val user = txn.newEntity("User")
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue").addLink("assignee", user)
        txn.flush()
        Assert.assertEquals(2, txn.getAll("Issue").selectManyDistinct("assignee").size())
    }

    fun testSelectMany() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user = txn.newEntity("User")
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue").addLink("assignee", user)
        txn.flush()
        Assert.assertEquals(3, txn.getAll("Issue").selectMany("assignee").size())
    }

    fun testSelectMany2() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user1 = txn.newEntity("User")
        val user2 = txn.newEntity("User")
        val issue1 = txn.newEntity("Issue")
        issue1.addLink("assignee", user1)
        issue1.addLink("assignee", user2)
        issue1.addLink("assignee", txn.newEntity("User"))
        val issue2 = txn.newEntity("Issue")
        issue2.addLink("assignee", user1)
        issue2.addLink("assignee", user2)
        txn.flush()
        Assert.assertEquals(6, txn.getAll("Issue").selectMany("assignee").size())
    }

    fun testSelectManyFromEmptySequence() {
        val txn = storeTransaction
        txn.newEntity("Issue")
        txn.newEntity("User")
        txn.flush()
        Assert.assertEquals(0, txn.getAll("Issue").intersect(txn.getAll("User")).selectMany("unknown_link").size())
    }

    fun testSelectMany3() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue").addLink("assignee", txn.newEntity("User"))
        val user1 = txn.newEntity("User")
        val user2 = txn.newEntity("User")
        val issue1 = txn.newEntity("Issue")
        issue1.addLink("assignee", user1)
        issue1.addLink("assignee", user2)
        issue1.addLink("assignee", txn.newEntity("User"))
        val issue2 = txn.newEntity("Issue")
        issue2.addLink("assignee", user1)
        issue2.addLink("assignee", user2)
        txn.newEntity("Issue")
        txn.flush()
        Assert.assertEquals(7, txn.getAll("Issue").selectMany("assignee").size())
    }

    fun testSelectManySource() {
        val txn = storeTransaction
        repeat(10000) { i ->
            txn.newEntity("Issue").apply {
                setProperty("i", i)
                addLink("assignee", txn.newEntity("User"))
                addLink("assignee", txn.newEntity("User"))
            }
        }
        txn.flush()
        val iterator = txn.getAll("Issue").selectMany("assignee").iterator() as SourceMappingIterator
        var i = 0
        iterator.forEach { _ ->
            Assert.assertEquals(i++ / 2, txn.getEntity(iterator.sourceId).getProperty("i"))
        }
    }

    fun testSelectManySingular() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.newEntity("Issue")
        txn.flush()
        Assert.assertEquals(0, txn.getAll("Issue").selectMany("assignee").size())
        entityStore.getLinkId(txn, "assignee", true)
        txn.flush()
        Assert.assertEquals(1, txn.getAll("Issue").selectMany("assignee").size())
        val user = txn.newEntity("User")
        txn.newEntity("Issue").addLink("assignee", user)
        txn.newEntity("Issue").addLink("assignee", user)
        txn.flush()
        Assert.assertEquals(3, txn.getAll("Issue").selectMany("assignee").size())
    }

    fun testFindLinks() {
        entityStore.config.isCachingDisabled = true
        val txn = storeTransaction
        val users = createNUsers(txn, 10)
        val nobody = txn.newEntity("User")
        txn.flush()
        for (i in 0..9) {
            val issue = txn.newEntity("Issue")
            issue.addLink("author", users[9 - i])
            if (9 - i in 3..8) {
                issue.addLink("author", users[8])
            }
        }
        txn.flush()
        val someUsers = txn.find("User", "login", "user3", "user8")
        val links0 = txn.findLinks("Issue", someUsers, "author")
        Assert.assertEquals(someUsers.size(), links0.size())
        val authoredIssues = txn.findWithLinks("Issue", "author")
        val links1 = (authoredIssues as EntityIterableBase).findLinks(toList(someUsers), "author")
        Assert.assertEquals(someUsers.size(), links1.size())
        val it0 = links0.iterator()
        val it1 = links1.iterator()
        while (it0.hasNext() || it1.hasNext()) {
            Assert.assertTrue(it0.hasNext())
            Assert.assertTrue(it1.hasNext())
            Assert.assertEquals(it0.nextId(), it1.nextId())
        }
        assertEquals(6, (txn.getAll("Issue") as EntityIterableBase).findLinks(someUsers, "author").size())
        assertEquals(6, (txn.getAll("Issue") as EntityIterableBase).findLinks(someUsers, "author").intersect(txn.getAll("Issue")).size())
        assertEquals(6, toList((txn.getAll("Issue") as EntityIterableBase).findLinks(someUsers, "author").intersect(txn.findWithLinks("Issue", "author"))).size)
        entityStore.asyncProcessor.waitForJobs(100)
        assertEquals(6, (txn.getAll("Issue") as EntityIterableBase).findLinks(someUsers, "author").size())
        entityStore.asyncProcessor.waitForJobs(100)
        for (issue in txn.getAll("Issue")) {
            issue.deleteLinks("author")
            issue.addLink("author", nobody)
        }
        assertEquals(0, (txn.getAll("Issue") as EntityIterableBase).findLinks(toList(someUsers), "author").size())
    }

    @TestFor(issue = "XD-730")
    fun testFindMultipleLinks() {
        val txn = storeTransaction
        createNUsers(txn, 10)
        val issue1 = txn.newEntity("Issue")
        issue1.addLink("author", txn.find("User", "login", "user0").first.notNull)
        val user1 = txn.find("User", "login", "user1").first
        issue1.addLink("author", user1.notNull)
        val issue2 = txn.newEntity("Issue")
        issue2.addLink("author", txn.find("User", "login", "user2").first.notNull)
        val user3 = txn.find("User", "login", "user3").first
        issue2.addLink("author", user3.notNull)
        txn.newEntity("Issue")
        txn.flush()
        assertEquals(2, (txn.getAll("Issue") as EntityIterableBase).findLinks(
                toList(txn.getSingletonIterable(user1.notNull).union(txn.getSingletonIterable(user3.notNull))), "author").size())
    }

    fun testFindLinksSingular() {
        val txn = storeTransaction
        createNUsers(txn, 1)
        val issue = txn.newEntity("Issue")
        issue.addLink("author", txn.find("User", "login", "user0").first.notNull)
        txn.flush()
        Assert.assertEquals(0L, EntityIterableBase.EMPTY.findLinks(txn.getAll("User"), "author").size())
        Assert.assertEquals(0L, (txn.getAll("Issue") as EntityIterableBase).findLinks(EntityIterableBase.EMPTY, "author").size())
    }

    @TestFor(issue = "XD-749")
    fun testFindLinksSingular2() {
        val txn = storeTransaction
        createNUsers(txn, 1)
        val issue = txn.newEntity("Issue")
        issue.addLink("author", txn.find("User", "login", "user0").first.notNull)
        txn.flush()
        Assert.assertEquals(0L, EntityIterableBase.EMPTY.findLinks(toList(txn.getAll("User")), "author").size())
        Assert.assertEquals(0L, (txn.getAll("Issue") as EntityIterableBase).findLinks(toList(EntityIterableBase.EMPTY), "author").size())
    }

    @TestFor(issue = "XD-737")
    fun testInvalidationOfCachedFindLinks() {
        val txn = storeTransaction
        val users = createNUsers(txn, 10)
        for (user in users) {
            user.setLink("inGroup", txn.newEntity("UserGroup"))
        }
        txn.getAll("UserGroup").first.notNull.setLink("owner", users[0])
        txn.flush()
        val withOwner = txn.findWithLinks("UserGroup", "owner")
        Assert.assertEquals(1, withOwner.size())
        val usersInGroupsWithOwner = (txn.getAll("User") as EntityIterableBase).findLinks(withOwner, "inGroup") as EntityIterableBase
        Assert.assertEquals(1, usersInGroupsWithOwner.size())
        //Assert.assertTrue(usersInGroupsWithOwner.isCached());
        txn.getAll("UserGroup").last.notNull.setLink("owner", users[users.size - 1])
        Assert.assertFalse(usersInGroupsWithOwner.isCached)
        txn.flush()
        Assert.assertEquals(2, withOwner.size())
        Assert.assertEquals(2, usersInGroupsWithOwner.size())
        //Assert.assertTrue(usersInGroupsWithOwner.isCached());
        txn.getAll("UserGroup").last.notNull.deleteLinks("owner")
        txn.flush()
        //Assert.assertFalse(usersInGroupsWithOwner.isCached());
        Assert.assertEquals(1, withOwner.size())
        Assert.assertEquals(1, usersInGroupsWithOwner.size())
    }

    fun testFindLinksReverse() {
        entityStore.config.isCachingDisabled = true
        val txn = storeTransaction
        val users = createNUsers(txn, 10)
        for (user in users) {
            user.setLink("inGroup", txn.newEntity("UserGroup"))
        }
        txn.getAll("UserGroup").first.notNull.setLink("owner", users[0])
        txn.getAll("UserGroup").last.notNull.setLink("owner", users[1])
        txn.flush()
        val withOwner = txn.findWithLinks("UserGroup", "owner")
        val all = txn.getAll("User") as EntityIterableBase
        val iterator = all.findLinks(withOwner, "inGroup").iterator()
        Assert.assertEquals(users[0], iterator.next())
        Assert.assertEquals(users[9], iterator.next())
        Assert.assertFalse(iterator.hasNext())
        val reverseIterator = all.findLinks(withOwner, "inGroup").reverse().iterator()
        Assert.assertEquals(users[9], reverseIterator.next())
        Assert.assertEquals(users[0], reverseIterator.next())
        Assert.assertFalse(reverseIterator.hasNext())
    }

    fun testFindLinksToReverse() {
        entityStore.config.isCachingDisabled = true
        val txn = storeTransaction
        val users = createNUsers(txn, 10)
        val profile = txn.newEntity("Profile")
        users.forEach { user ->
            user.setLink("profile", profile)
        }
        val group = txn.newEntity("UserGroup")
        // make sure id for the link "inGroup" is created
        users[0].setLink("inGroup", group)
        users[0].deleteLink("inGroup", group)
        txn.flush()
        var inGroup = txn.findLinks("User", group, "inGroup").reverse()
        Assert.assertEquals(0, inGroup.toList().size)
        users.forEach { user ->
            user.setLink("inGroup", group)
        }
        txn.flush()
        inGroup = txn.findLinks("User", group, "inGroup")
        Assert.assertEquals(users.size, inGroup.size().toInt())
        inGroup.forEachIndexed { i, user ->
            Assert.assertEquals(users[i], user)
        }
        inGroup.reverse().forEachIndexed { i, user ->
            Assert.assertEquals(users[users.size - i - 1], user)
        }
    }

    @TestFor(issue = "XD-826")
    fun testIntersectOfFindLinks() {
        val txn = storeTransaction
        val users = createNUsers(txn, 10)
        val group0 = txn.newEntity("UserGroup")
        val group1 = txn.newEntity("UserGroup")
        (0..5).forEach { i ->
            users[i].addLink("group", group0)
        }
        (4..9).forEach { i ->
            users[i].addLink("group", group1)
        }
        val all = txn.getAll("User") as EntityIterableBase
        val result = all.findLinks(listOf(group0), "group").intersect(
                all.findLinks(listOf(group1), "group"))
        Assert.assertEquals(2L, result.size())
        Assert.assertEquals(users[4], result.first)
        Assert.assertEquals(users[5], result.last)
    }

    fun testGetFirst() {
        for (i in 0..255) {
            Assert.assertNull(EntityIterableBase.EMPTY.first)
        }
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.flush()
        Assert.assertNotNull(txn.getAll("User").first)
        Assert.assertEquals("user0", txn.getAll("User").first.notNull.getProperty("login"))
    }

    fun testGetLast() {
        for (i in 0..255) {
            Assert.assertNull(EntityIterableBase.EMPTY.last)
        }
        val txn = storeTransaction
        createNUsers(txn, 10)
        txn.flush()
        Assert.assertNotNull(txn.getAll("User").last)
        Assert.assertEquals("user9", txn.getAll("User").last.notNull.getProperty("login"))
    }

    fun testGetLastOfGetAll() {
        val txn = storeTransaction
        val count = 100000
        createNUsers(txn, count)
        txn.flush()
        val started = System.currentTimeMillis()
        val lastUser = txn.getAll("User").last
        println(System.currentTimeMillis() - started)
        Assert.assertNotNull(lastUser)
        Assert.assertEquals("user" + (count - 1), lastUser.notNull.getProperty("login"))
    }

    fun testGetLastOfToLinks() {
        val txn = storeTransaction
        val count = 1000
        val users = createNUsers(txn, count)
        txn.flush()
        val group = txn.newEntity("UserGroup")
        txn.flush()
        Assert.assertNull(txn.findLinks("User", group, "inGroup").last)
        users.first().addLink("inGroup0", group)
        txn.flush()
        Assert.assertNull(txn.findLinks("User", group, "inGroup").last)
        users.first().addLink("inGroup", group)
        txn.flush()
        Assert.assertEquals(users.first(), txn.findLinks("User", group, "inGroup").last)
        users.first().deleteLink("inGroup", group)
        txn.flush()
        Assert.assertNull(txn.findLinks("User", group, "inGroup").last)
        users.first().addLink("inGroup2", group)
        txn.flush()
        Assert.assertNull(txn.findLinks("User", group, "inGroup").last)
        users.copyOfRange(2, count).forEach { it.addLink("inGroup", group) }
        users[0].addLink("inGroup", txn.newEntity("UserGroup"))
        users[1].addLink("inGroup", txn.newEntity("UserGroup2"))
        txn.flush()
        Assert.assertEquals(users.last(), txn.findLinks("User", group, "inGroup").last)
    }

    fun testGetLastOfFromLinks() {
        val txn = storeTransaction
        val count = 1000
        val users = createNUsers(txn, count)
        txn.flush()
        val group = txn.newEntity("UserGroup")
        txn.flush()
        Assert.assertNull(group.getLinks("users").last)
        group.addLink("users0", users.first())
        txn.flush()
        Assert.assertNull(group.getLinks("users").last)
        group.addLink("users", users.first())
        txn.flush()
        Assert.assertEquals(users.first(), group.getLinks("users").last)
        group.deleteLink("users", users.first())
        txn.flush()
        Assert.assertNull(group.getLinks("users").last)
        group.addLink("users2", users.first())
        txn.flush()
        Assert.assertNull(group.getLinks("users").last)
        users.forEach { group.addLink("users", it) }
        txn.flush()
        Assert.assertEquals(users.last(), group.getLinks("users").last)
    }

    fun testSingleEntityIterable_XD_408() {
        val txn = storeTransaction
        val count = 1
        createNUsers(txn, count)
        txn.flush()
        var users = txn.getSingletonIterable(txn.getAll("User").first.notNull)
        users = users.union(users)
        Assert.assertFalse((users as EntityIterableBase).canBeCached())
        Assert.assertEquals(1L, users.getRoughSize())
    }

    fun testSingleEntityIterable2() {
        val txn = storeTransaction
        val count = 1
        createNUsers(txn, count)
        txn.flush()
        var users = txn.getSingletonIterable(txn.getAll("User").first.notNull)
        users = users.union(users)
        Assert.assertFalse((users as EntityIterableBase).canBeCached())
        Assert.assertEquals(-1L, users.getRoughCount())
        entityStore.asyncProcessor.waitForJobs(100)
        Assert.assertEquals(1L, users.getRoughCount())
    }

    @TestFor(issue = "XD-502")
    fun testFindWithPropSortedCount() {
        val txn = storeTransaction
        Assert.assertEquals(0, txn.findWithPropSortedByValue("User", "login").countImpl(txn))
        val count = 10
        createNUsers(txn, count)
        txn.flush()
        Assert.assertEquals(count.toLong(), txn.findWithPropSortedByValue("User", "login").countImpl(txn))
    }

    fun testCachedInstanceIsEmpty() {
        val txn = storeTransaction
        val count = 10
        createNUsers(txn, count)
        txn.flush()
        val store = entityStore
        Assert.assertEquals(0, txn.getAll("User").indexOf(PersistentEntity(store, PersistentEntityId(0, 0))).toLong())
        val cachedInstance = store.entityIterableCache.putIfNotCached(txn.getAll("User") as EntityIterableBase)
        Assert.assertFalse(cachedInstance.isEmpty)
    }

    @TestFor(issue = "XD-522")
    @Throws(InterruptedException::class)
    fun testRoughSize() {
        val txn = storeTransaction
        val count = 10
        createNUsers(txn, count)
        txn.flush()
        val allUsers = txn.findStartingWith("User", "login", "u")
        Assert.assertEquals(10L, allUsers.roughSize)
        Thread.sleep(1000)
        for (i in 0..2) {
            createNUsers(txn, 1)
            txn.flush()
            Assert.assertEquals(10L + i, allUsers.roughSize)
            Thread.sleep(1000)
        }
    }

    @TestFor(issue = "XD-536")
    fun testEntityIterableCacheIsInvalidatedOnStoreClear() {
        val entityStore = entityStore
        entityStore.executeInTransaction { txn -> createNUsers(txn, 10) }
        entityStore.executeInReadonlyTransaction { txn -> Assert.assertEquals(9, txn.getAll("User").indexOf(PersistentEntity(entityStore, PersistentEntityId(0, 9))).toLong()) }
        entityStore.clear()
        entityStore.executeInTransaction { txn -> createNUsers(txn, 1) }
        entityStore.executeInReadonlyTransaction { txn -> Assert.assertEquals(-1, txn.getAll("User").indexOf(PersistentEntity(entityStore, PersistentEntityId(0, 9))).toLong()) }
    }

    @TestFor(issue = "XD-746")
    fun testGetAllReverse() {
        val txn = storeTransaction
        val count = 10
        createNUsers(txn, count)
        txn.flush()
        var i = 0
        for (user in txn.getAll("User").reverse()) {
            Assert.assertEquals("user" + (10 - ++i), user.getProperty("login"))
        }
        Assert.assertEquals(count.toLong(), i.toLong())
    }

    fun testCachingInReadonlyTxn() {
        val txn = storeTransaction
        val users = createNUsers(txn, 10)
        txn.flush()
        assertFalse(txn.findWithProp("User", "login").isCached)
        entityStore.executeInReadonlyTransaction { tx ->
            tx.findWithProp("User", "login").contains(users[0])
            assertTrue((tx as PersistentStoreTransaction).findWithProp("User", "login").isCached)
        }
        assertTrue(txn.findWithProp("User", "login").isCached)
        createNUsers(txn, 10)
        txn.flush()
        assertTrue(txn.findWithProp("User", "login").isCached)
    }

    private fun checkIdRange(issues: EntityIterable, lo: Int, hi: Int) {
        var i = lo
        for (e in issues) {
            assertEquals(i.toLong(), e.id.localId)
            assertEquals(if (i and 1 == 0) 50 else 100, e.getProperty("size"))
            i++
        }
        assertEquals(hi + 1, i)
    }

    private fun createNUsers(txn: StoreTransaction, n: Int): Array<Entity> {
        val result = arrayOfNulls<Entity>(n)
        for (i in 0 until n) {
            val user = txn.newEntity("User")
            user.setProperty("login", "user$i")
            result[i] = user
        }
        @Suppress("UNCHECKED_CAST")
        return result as Array<Entity>
    }

    private fun EntityIterable.selectMany(linkName: String) =
            (this as EntityIterableBase).selectMany(linkName)
}