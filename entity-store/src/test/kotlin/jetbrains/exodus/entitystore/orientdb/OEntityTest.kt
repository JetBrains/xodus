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
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.metadata.schema.OType
import jetbrains.exodus.entitystore.EntityRemovedInDatabaseException
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.testutil.*
import jetbrains.exodus.entitystore.orientdb.testutil.createIssueImpl
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream
import java.util.*
import kotlin.random.Random
import kotlin.test.*

class OEntityTest: OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `create entities`() {
        val (e1, e2) = orientDb.withStoreTx { tx ->
            val e1 = tx.newEntity(Issues.CLASS)
            val e2 = tx.newEntity(Issues.CLASS)
            assertEquals(tx.getTypeId(Issues.CLASS), e1.id.typeId)
            assertEquals(tx.getTypeId(Issues.CLASS), e2.id.typeId)
            assertEquals(0, e1.id.localId)
            assertEquals(1, e2.id.localId)
            assertEquals(2, tx.getAll(Issues.CLASS).size())

            assertEquals(e1, tx.getEntity(e1.id))
            assertEquals(e2, tx.getEntity(e2.id))

            Pair(e1, e2)
        }

        orientDb.withStoreTx { tx ->
            assertTrue(e1.delete())
        }

        orientDb.withStoreTx { tx ->
            tx.getEntity(e2.id)
            assertFailsWith<EntityRemovedInDatabaseException> { tx.getEntity(e1.id) }
            assertEquals(1, tx.getAll(Issues.CLASS).size())
        }
    }

    @Test
    fun `an entity sees changes made to it in another part of the application`() {
        // your entity
        val e1 = orientDb.withStoreTx { tx ->
            val e1 = tx.newEntity(Issues.CLASS)
            e1.setProperty("name", "Pumba")
            e1
        }

        // this changes happen in another part of the application
        val e1Again = orientDb.withStoreTx { tx ->
            val e1Again = tx.getEntity(e1.id)
            e1Again.setProperty("name", "Bampu")
            e1Again
        }

        // make sure we do not deal with physically the same instance
        assertNotSame(e1, e1Again)

        // your entity sees changes made in another part of the application
        orientDb.withStoreTx { tx ->
            assertEquals("Bampu", e1.getProperty("name"))
        }
    }


    @Test
    fun `rename entity type`() {
        orientDb.withStoreTx { tx ->
            for (i in 0..9) {
                tx.newEntity("Issue")
            }
            assertEquals(10, tx.getAll("Issue").size())
        }
        orientDb.withStoreTx {
            orientDb.store.renameEntityType("Issue", "Comment")
        }
        orientDb.withStoreTx { tx ->
            assertEquals(10, tx.getAll("Comment").size())
        }
    }

    @Test
    fun `multiple links should work`() {
        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        orientDb.withStoreTx {
            issueA.addLink(linkName, issueB)
            issueA.addLink(linkName, issueC)
        }

        orientDb.withStoreTx {
            val links = issueA.getLinks(linkName)
            assertTrue(links.contains(issueB))
            assertTrue(links.contains(issueC))
        }
    }

    /**
     * Orient may once in a while go crazy and add random bytes to
     * a blob. It does not like blobs of size 1023, 2047, 4095, 8191 and so on.
     *
     * One should NOT use fromInputStream() in the blob implementation is fixed.
     *
     * This test checks that our implementation behaves.
     */
    @Test
    fun `set a hard blob`() {
        val hardBlob = Random.nextBytes(1023)
        val id = orientDb.withStoreTx { tx ->
            val issue = tx.createIssueImpl("iss")
            issue.setBlob("blob1", ByteArrayInputStream(hardBlob))
            issue.id
        }

        orientDb.withStoreTx { tx ->
            val issue = tx.getEntity(id)
            val gotBlob = issue.getBlob("blob1")!!.readAllBytes()
            assertContentEquals(hardBlob, gotBlob)
        }
    }

    @Test
    fun `set, change and delete blobs`() {
        val issue = orientDb.createIssue("iss")

        // set
        val expectedBlob1 = byteArrayOf(0x01, 0x02)
        val expectedBlob2 = byteArrayOf(0x04, 0x05, 0x06)
        orientDb.withStoreTx {
            issue.setBlob("blob1", ByteArrayInputStream(expectedBlob1))
            issue.setBlob("blob2", ByteArrayInputStream(expectedBlob2))
        }
        orientDb.withStoreTx {
            assertContentEquals(expectedBlob1, issue.getBlob("blob1")!!.readAllBytes())
            assertContentEquals(expectedBlob2, issue.getBlob("blob2")!!.readAllBytes())
            assertEquals(expectedBlob1.size.toLong(), issue.getBlobSize("blob1"))
            assertEquals(expectedBlob2.size.toLong(), issue.getBlobSize("blob2"))
        }

        // change
        val expectedResetBlob1 = byteArrayOf(0x01, 0x03, 0x04, 0x05)
        orientDb.withStoreTx {
            issue.setBlob("blob1", ByteArrayInputStream(expectedResetBlob1))
        }
        orientDb.withStoreTx {
            val resetBlob1 = issue.getBlob("blob1")!!.readAllBytes()
            assertContentEquals(expectedResetBlob1, resetBlob1)
            assertEquals(expectedResetBlob1.size.toLong(), issue.getBlobSize("blob1"))
        }

        // delete
        orientDb.withStoreTx {
            issue.deleteBlob("blob1")
        }
        orientDb.withStoreTx {
            assertNull(issue.getBlob("blob1"))
            assertEquals(-1, issue.getBlobSize("blob1"))
            // another blob is still here
            assertContentEquals(expectedBlob2, issue.getBlob("blob2")!!.readAllBytes())
            assertEquals(expectedBlob2.size.toLong(), issue.getBlobSize("blob2"))
        }
    }

    @Test
    fun `set, change and delete string blobs`() {
        val issue = orientDb.createIssue("iss")

        // set
        val expectedBlob1 = "Abc"
        val expectedBlob2 = "dxYz"
        orientDb.withStoreTx {
            issue.setBlobString("blob1", expectedBlob1)
            issue.setBlobString("blob2", expectedBlob2)
        }
        orientDb.withStoreTx {
            assertEquals(expectedBlob1, issue.getBlobString("blob1"))
            assertEquals(expectedBlob2, issue.getBlobString("blob2"))
            assertEquals(expectedBlob1.length.toLong() + 2, issue.getBlobSize("blob1"))
            assertEquals(expectedBlob2.length.toLong() + 2, issue.getBlobSize("blob2"))
        }

        // change
        val expectedResetBlob1 = "Caramba"
        orientDb.withStoreTx {
            issue.setBlobString("blob1", expectedResetBlob1)
        }
        orientDb.withStoreTx {
            val resetBlob1 = issue.getBlobString("blob1")
            assertEquals(expectedResetBlob1, resetBlob1)
            assertEquals(expectedResetBlob1.length.toLong() + 2, issue.getBlobSize("blob1"))
        }

        // delete
        orientDb.withStoreTx {
            issue.deleteBlob("blob1")
        }
        orientDb.withStoreTx {
            assertNull(issue.getBlobString("blob1"))
            assertEquals(-1, issue.getBlobSize("blob1"))
            // another blob is still here
            assertEquals(expectedBlob2, issue.getBlobString("blob2"))
            assertEquals(expectedBlob2.length.toLong() + 2, issue.getBlobSize("blob2"))
        }
    }

    @Test
    fun `string blobs size`() {
        val issue = orientDb.createIssue("iss")

        // set
        val englishStr = "mamba, mamba, caramba"
        val notEnglishStr = "вы хотите песен, их есть у меня"
        val mixedStr = "magic пипл woodoo пипл"
        orientDb.withStoreTx {
            issue.setBlobString("blob1", englishStr)
            issue.setBlobString("blob2", notEnglishStr)
            issue.setBlobString("blob3", mixedStr)
        }
        orientDb.withStoreTx {
            assertEquals(englishStr, issue.getBlobString("blob1"))
            assertEquals(notEnglishStr, issue.getBlobString("blob2"))
            assertEquals(mixedStr, issue.getBlobString("blob3"))

            // we use modified UTF-8 for string blobs, it adds the string size to 2 first bytes
            assertEquals(englishStr.length.toLong() + 2, issue.getBlobSize("blob1"))

            assertNotEquals(notEnglishStr.length.toLong(), issue.getBlobSize("blob2"))
            assertNotEquals(notEnglishStr.length.toLong(), issue.getBlobSize("blob2"))
            assertEquals(57, issue.getBlobSize("blob2"))

            assertEquals(32, issue.getBlobSize("blob3"))
        }
    }

    @Test
    fun `add blob should be reflected in get blob names`() {
        val issue = orientDb.createIssue("TestBlobs")

        orientDb.withStoreTx {
            issue.setBlob("blob1", ByteArrayInputStream(byteArrayOf(0x01, 0x02, 0x03)))
            issue.setBlob("blob2", ByteArrayInputStream(byteArrayOf(0x04, 0x05, 0x06)))
            issue.setBlob("blob3", ByteArrayInputStream(byteArrayOf(0x07, 0x08, 0x09)))
            issue.setProperty("version", 99)
        }

        orientDb.withStoreTx {
            val blobNames = issue.getBlobNames()
            assertTrue(blobNames.contains("blob1"))
            assertTrue(blobNames.contains("blob2"))
            assertTrue(blobNames.contains("blob3"))
            assertEquals(3, blobNames.size)
        }
    }

    @Test
    fun `set the same string blob should return false`() {
        val issue = orientDb.createIssue("GetPropertyTest")

        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withStoreTx {
            issue.setBlobString(propertyName, propertyValue)
        }
        orientDb.withStoreTx {
            assertEquals(false, issue.setBlobString(propertyName, propertyValue))
        }
    }

    @Test
    fun `delete links`() {
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
            val oClass = session.getClass(Issues.CLASS)!!
            // pretend that the link is indexed
            oClass.createProperty(linkTargetEntityIdPropertyName(linkName), OType.LINKBAG)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")
        val issueD = orientDb.createIssue("D")

        orientDb.withStoreTx {
            issueA.addLink(linkName, issueB)
            issueA.addLink(linkName, issueC)
            issueA.addLink(linkName, issueD)
        }

        orientDb.withStoreTx {
            issueA.deleteLink(linkName, issueB)
            issueA.deleteLink(linkName, issueC.id)

            val links = issueA.getLinks(linkName)
            assertEquals(1, links.size())
            assertTrue(links.any { it.id == issueD.id })

            val bag = issueA.vertex.getTargetLocalEntityIds(linkName)
            assertEquals(1, bag.size())
            assertTrue(bag.contains(issueD.vertex.identity))
        }
    }

    @Test
    fun `set links`() {
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
            val oClass = session.getClass(Issues.CLASS)!!
            // pretend that the link is indexed
            oClass.createProperty(linkTargetEntityIdPropertyName(linkName), OType.LINKBAG)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        orientDb.withStoreTx {
            assertTrue(issueA.setLink(linkName, issueB))
            assertFalse(issueA.setLink(linkName, issueB))

            assertEquals(issueB, issueA.getLink(linkName))
            val bag = issueA.vertex.getTargetLocalEntityIds(linkName)
            assertEquals(1, bag.size())
            assertTrue(bag.contains(issueB.vertex.identity))
        }

        orientDb.withStoreTx {
            assertTrue(issueA.setLink(linkName, issueC))

            assertEquals(issueC, issueA.getLink(linkName))
            val bag = issueA.vertex.getTargetLocalEntityIds(linkName)
            assertEquals(1, bag.size())
            assertTrue(bag.contains(issueC.vertex.identity))
        }

        orientDb.withStoreTx {
            assertTrue(issueA.setLink(linkName, issueB.id))
            assertFalse(issueA.setLink(linkName, issueB.id))

            assertEquals(issueB, issueA.getLink(linkName))
            val bag = issueA.vertex.getTargetLocalEntityIds(linkName)
            assertEquals(1, bag.size())
            assertTrue(bag.contains(issueB.vertex.identity))
        }
    }

    @Test
    fun `should delete all links`() {
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
            val oClass = session.getClass(Issues.CLASS)!!
            // pretend that the link is indexed
            oClass.createProperty(linkTargetEntityIdPropertyName(linkName), OType.LINKBAG)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        orientDb.withStoreTx {
            issueA.addLink(linkName, issueB)
            issueA.addLink(linkName, issueC)
        }

        orientDb.withStoreTx {
            issueA.deleteLinks(linkName)
            val links = issueA.getLinks(linkName)
            assertEquals(0, links.size())
            // the complementary property must also be cleared
            val bag = issueA.vertex.getTargetLocalEntityIds(linkName)
            assertEquals(0, bag.size())
        }
    }

    @Test
    fun `should replace a link correctly`() {
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        orientDb.withStoreTx {
            issueA.setLink(linkName, issueB.id)
        }
        orientDb.withStoreTx {
            assertEquals(issueB, issueA.getLink(linkName))
        }
        orientDb.withStoreTx {
            issueA.setLink(linkName, issueC.id)
        }
        orientDb.withStoreTx {
            assertEquals(issueC, issueA.getLink(linkName))
        }
    }

    @Test
    fun `setLink() and addLink() should work correctly with PersistentEntityId`() {
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        orientDb.withStoreTx {
            val legacyId = PersistentEntityId(issueB.id.typeId, issueB.id.localId)
            issueA.setLink(linkName, legacyId)
        }
        orientDb.withStoreTx {
            assertEquals(issueB, issueA.getLink(linkName))
        }
        orientDb.withStoreTx {
            val legacyId = PersistentEntityId(issueC.id.typeId, issueC.id.localId)
            issueB.addLink(linkName, legacyId)
        }
        orientDb.withStoreTx {
            assertEquals(issueB, issueA.getLink(linkName))
        }
    }

    @Test
    fun `setLink() and addLink() return false if the target entity is not found`() {
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        val issueB = orientDb.createIssue("A")

        orientDb.withStoreTx { tx ->
            issueB.delete()
        }

        orientDb.withStoreTx {
            assertFalse(issueB.addLink(linkName, issueB.id))
            assertFalse(issueB.addLink(linkName, ORIDEntityId.EMPTY_ID))
            assertFalse(issueB.addLink(linkName, PersistentEntityId.EMPTY_ID))
            assertFalse(issueB.addLink(linkName, PersistentEntityId(300, 300)))
        }

        orientDb.withStoreTx {
            assertFalse(issueB.setLink(linkName, issueB.id))
            assertFalse(issueB.setLink(linkName, ORIDEntityId.EMPTY_ID))
            assertFalse(issueB.setLink(linkName, PersistentEntityId.EMPTY_ID))
            assertFalse(issueB.setLink(linkName, PersistentEntityId(300, 300)))
        }
    }

    @Test
    fun `should get property`() {
        val issue = orientDb.createIssue("GetPropertyTest")

        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withStoreTx {
            issue.setProperty(propertyName, propertyValue)
        }
        orientDb.withStoreTx {
            val value = issue.getProperty(propertyName)
            assertEquals(propertyValue, value)
        }
    }

    @Test
    fun `should delete property`() {
        val issue = orientDb.createIssue("DeletePropertyTest")

        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withStoreTx {
            issue.setProperty(propertyName, propertyValue)
        }
        orientDb.withStoreTx {
            issue.deleteProperty(propertyName)
            val value = issue.getProperty(propertyName)
            assertNull(value)
        }
    }

    @Test
    fun `set, read, change and delete properties`() {
        val issue = orientDb.createIssue("Test1")

        orientDb.withStoreTx {
            issue.setProperty("hello", "world")
            issue.setProperty("june", 6)
            issue.setProperty("year", 44L)
            issue.setProperty("floatProp", 1.3f)
            issue.setProperty("doubleProp", 2.3)
            issue.setProperty("dateProp", Date(300))
            issue.setProperty("boolProp", true)
        }

        orientDb.withStoreTx {
            assertEquals("world", issue.getProperty("hello"))
            assertEquals(6, issue.getProperty("june"))
            assertEquals(44L, issue.getProperty("year"))
            assertEquals(1.3f, issue.getProperty("floatProp"))
            assertEquals(2.3, issue.getProperty("doubleProp"))
            assertEquals(Date(300), issue.getProperty("dateProp"))
            assertEquals(true, issue.getProperty("boolProp"))
        }

        orientDb.withStoreTx {
            assertEquals(false, issue.setProperty("hello", "world"))
            assertEquals(false, issue.setProperty("june", 6))
            assertEquals(false, issue.setProperty("year", 44L))
            assertEquals(false, issue.setProperty("floatProp", 1.3f))
            assertEquals(false, issue.setProperty("doubleProp", 2.3))
            assertEquals(false, issue.setProperty("dateProp", Date(300)))
            assertEquals(false, issue.setProperty("boolProp", true))
        }

        orientDb.withStoreTx {
            assertEquals(true, issue.setProperty("hello", "xodus"))
            assertEquals(true, issue.setProperty("june", 8))
            assertEquals(true, issue.setProperty("year", 34L))
            assertEquals(true, issue.setProperty("floatProp", 2.3f))
            assertEquals(true, issue.setProperty("doubleProp", 4.3))
            assertEquals(true, issue.setProperty("dateProp", Date(303)))
            assertEquals(true, issue.setProperty("boolProp", false))
        }

        orientDb.withStoreTx {
            assertEquals("xodus", issue.getProperty("hello"))
            assertEquals(8, issue.getProperty("june"))
            assertEquals(34L, issue.getProperty("year"))
            assertEquals(2.3f, issue.getProperty("floatProp"))
            assertEquals(4.3, issue.getProperty("doubleProp"))
            assertEquals(Date(303), issue.getProperty("dateProp"))
            assertEquals(false, issue.getProperty("boolProp"))
        }


        orientDb.withStoreTx {
            issue.deleteProperty("dateProp")
            assertNull(issue.getProperty("dateProp"))
            // check that other properties are still there
            assertEquals("xodus", issue.getProperty("hello"))
        }

        orientDb.withStoreTx {
            assertEquals(
                listOf("hello", "name", "june", "year", "floatProp", "doubleProp", "boolProp").sorted(),
                issue.propertyNames.sorted()
            )
        }
    }

    @Test
    fun `it is forbidden to use entities outside transactions, except for id`() {
        val iss = orientDb.createIssue("trista")
        val anotherIss = orientDb.createIssue("sto")

        // no properties
        assertFailsWith<IllegalStateException> { iss.getProperty("name") }
        assertFailsWith<IllegalStateException> { iss.setProperty("name", "dvesti") }
        assertFailsWith<IllegalStateException> { iss.propertyNames }
        assertFailsWith<IllegalStateException> { iss.deleteProperty("name") }
        assertFailsWith<IllegalStateException> { iss.getRawProperty("name") }

        // no blobs
        assertFailsWith<IllegalStateException> { iss.getBlob("blob1") }
        assertFailsWith<IllegalStateException> { iss.getBlobSize("blob1") }
        assertFailsWith<IllegalStateException> { iss.getBlobString("blob1") }
        assertFailsWith<IllegalStateException> { iss.setBlob("blob1", ByteArrayInputStream(byteArrayOf(100))) }
        assertFailsWith<IllegalStateException> { iss.setBlobString("blob1", "opca") }
        assertFailsWith<IllegalStateException> { iss.deleteBlob("blob1") }

        // no links
        assertFailsWith<IllegalStateException> { iss.getLink("link1") }
        assertFailsWith<IllegalStateException> { iss.linkNames }
        assertFailsWith<IllegalStateException> { iss.setLink("link1", anotherIss) }
        assertFailsWith<IllegalStateException> { iss.deleteLink("link1",anotherIss) }
        assertFailsWith<IllegalStateException> { iss.deleteLinks("link1") }
        assertFailsWith<IllegalStateException> { iss.getLinks("link1") }
        assertFailsWith<IllegalStateException> { iss.getLinks(listOf("link1")); }

        // getting id is ok
        iss.id
    }

    @Test
    fun `dummy unique entityID_localId test`() {
        val localIdSet = hashSetOf<Long>()
        val typeIdSet = hashSetOf<Int>()
        (0..1000).map {
            val issue = orientDb.createIssue("Issue$it")
            typeIdSet.add(issue.id.typeId)
            localIdSet.add(issue.id.localId)
        }
        assertEquals(1001, localIdSet.size)
        assertEquals(1, typeIdSet.size)
    }

    @Test
    fun `setProperty and setBlobString returns false in case of equal values`(){
        val iss = orientDb.createIssue("trista")
        withStoreTx { tx ->
            iss.setProperty("test", 1)
            iss.setBlobString("blobString", "hello")
        }
        withStoreTx { tx ->
            assertEquals(false, iss.setProperty("test", 1))
            assertEquals(false, iss.setBlobString("blobString", "hello"))
        }
    }

    @Test
    fun `add new link types in a transaction`() {
        withStoreTx { tx ->
            val iss1 = tx.createIssue("iss1")
            val iss2 = tx.createIssue("iss2")

            iss1.addLink("trista", iss2)
        }
    }
}
