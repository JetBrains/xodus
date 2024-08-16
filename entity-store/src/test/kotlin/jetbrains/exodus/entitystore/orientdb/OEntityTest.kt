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
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.linkTargetEntityIdPropertyName
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import junit.framework.TestCase.assertFalse
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream
import kotlin.test.*

class OEntityTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

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
            Assert.assertTrue(links.contains(issueB))
            Assert.assertTrue(links.contains(issueC))
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

            assertEquals(englishStr.length.toLong() + 2, issue.getBlobSize("blob1"))

            assertNotEquals(notEnglishStr.length.toLong() + 2, issue.getBlobSize("blob2"))
            assertNotEquals(notEnglishStr.length.toLong() * 2 + 2, issue.getBlobSize("blob2"))
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
            Assert.assertTrue(blobNames.contains("blob1"))
            Assert.assertTrue(blobNames.contains("blob2"))
            Assert.assertTrue(blobNames.contains("blob3"))
            Assert.assertEquals(3, blobNames.size)
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
            Assert.assertEquals(false, issue.setBlobString(propertyName, propertyValue))
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
            Assert.assertEquals(issueB, issueA.getLink(linkName))
        }
        orientDb.withStoreTx {
            issueA.setLink(linkName, issueC.id)
        }
        orientDb.withStoreTx {
            Assert.assertEquals(issueC, issueA.getLink(linkName))
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
            Assert.assertEquals(issueB, issueA.getLink(linkName))
        }
        orientDb.withStoreTx {
            val legacyId = PersistentEntityId(issueC.id.typeId, issueC.id.localId)
            issueB.addLink(linkName, legacyId)
        }
        orientDb.withStoreTx {
            Assert.assertEquals(issueB, issueA.getLink(linkName))
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
            tx.delete(issueB.id.asOId())
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
            Assert.assertEquals(propertyValue, value)
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
            Assert.assertNull(value)
        }
    }

    @Test
    fun `should work with properties`() {
        val issue = orientDb.createIssue("Test1")

        orientDb.withStoreTx {
            issue.setProperty("hello", "world")
            issue.setProperty("june", 6)
            issue.setProperty("year", 44L)
        }

        orientDb.withStoreTx {
            Assert.assertEquals("world", issue.getProperty("hello"))
            Assert.assertEquals(6, issue.getProperty("june"))
            Assert.assertEquals(44L, issue.getProperty("year"))
        }

        orientDb.withStoreTx {
            Assert.assertEquals(false, issue.setProperty("hello", "world"))
            Assert.assertEquals(false, issue.setProperty("june", 6))
            Assert.assertEquals(false, issue.setProperty("year", 44L))
        }

        orientDb.withStoreTx {
            Assert.assertEquals(listOf(OVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME, "hello", "name", "june", "year").sorted(), issue.propertyNames.sorted())
        }
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
        Assert.assertEquals(1001, localIdSet.size)
        Assert.assertEquals(1, typeIdSet.size)
    }
}
