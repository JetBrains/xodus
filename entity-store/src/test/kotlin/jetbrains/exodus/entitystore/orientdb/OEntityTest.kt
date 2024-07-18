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
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
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
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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
        orientDb.provider.acquireSession().use { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        orientDb.withTxSession {
            issueA.addLink(linkName, issueB)
            issueA.addLink(linkName, issueC)
        }

        orientDb.withTxSession {
            val links = issueA.getLinks(linkName)
            Assert.assertTrue(links.contains(issueB))
            Assert.assertTrue(links.contains(issueC))
        }
    }

    @Test
    fun `add blob should be reflected in get blob names`() {
        val issue = orientDb.createIssue("TestBlobs")

        orientDb.withTxSession {
            issue.setBlob("blob1", ByteArrayInputStream(byteArrayOf(0x01, 0x02, 0x03)))
            issue.setBlob("blob2", ByteArrayInputStream(byteArrayOf(0x04, 0x05, 0x06)))
            issue.setBlob("blob3", ByteArrayInputStream(byteArrayOf(0x07, 0x08, 0x09)))
            issue.setProperty("version", 99)
        }

        orientDb.withTxSession {
            val blobNames = issue.getBlobNames()
            Assert.assertTrue(blobNames.contains("blob1"))
            Assert.assertTrue(blobNames.contains("blob2"))
            Assert.assertTrue(blobNames.contains("blob3"))
            Assert.assertEquals(3, blobNames.size)
        }
    }

    @Test
    fun `delete links`() {
        val linkName = "link"
        orientDb.provider.acquireSession().use { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
            val oClass = session.getClass(Issues.CLASS)!!
            // pretend that the link is indexed
            oClass.createProperty(linkTargetEntityIdPropertyName(linkName), OType.LINKBAG)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")
        val issueD = orientDb.createIssue("D")

        orientDb.withTxSession {
            issueA.addLink(linkName, issueB)
            issueA.addLink(linkName, issueC)
            issueA.addLink(linkName, issueD)
        }

        orientDb.withTxSession {
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
    fun `should delete all links`() {
        val linkName = "link"
        orientDb.provider.acquireSession().use { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
            val oClass = session.getClass(Issues.CLASS)!!
            // pretend that the link is indexed
            oClass.createProperty(linkTargetEntityIdPropertyName(linkName), OType.LINKBAG)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        orientDb.withTxSession {
            issueA.addLink(linkName, issueB)
            issueA.addLink(linkName, issueC)
        }

        orientDb.withTxSession {
            issueA.deleteLinks(linkName)
            val links = issueA.getLinks(linkName)
            assertEquals(0, links.size())
            // the complementary property must also be cleared
            val bag = issueA.vertex.getTargetLocalEntityIds(linkName)
            assertEquals(0, bag.size())
        }
    }

    @Test
    fun `should delete element holding blob after blob deleted from entity`() {
        val issue = orientDb.createIssue("TestBlobDelete")

        val linkName = "blobForDeletion"
        orientDb.withTxSession {
            issue.setBlob(linkName, ByteArrayInputStream(byteArrayOf(0x11, 0x12, 0x13)))
        }
        val blob = orientDb.withSession {
            issue.vertex.reload<OVertex>()
            issue.vertex.getLinkProperty(linkName)!!
        }

        orientDb.withTxSession {
            Assert.assertNotNull(it.load<OElement>(blob.identity))
            issue.deleteBlob(linkName)
        }

        orientDb.withTxSession {
            val blobNames = issue.getBlobNames()
            Assert.assertFalse(blobNames.contains(linkName))
            Assert.assertEquals(null, it.load<OElement>(blob.identity))
        }
    }

    @Test
    fun `should replace a link correctly`() {
        val linkName = "link"
        orientDb.provider.acquireSession().use { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        orientDb.withTxSession {
            issueA.setLink(linkName, issueB.id)
        }
        orientDb.withSession {
            Assert.assertEquals(issueB, issueA.getLink(linkName))
        }
        orientDb.withTxSession {
            issueA.setLink(linkName, issueC.id)
        }
        orientDb.withSession {
            Assert.assertEquals(issueC, issueA.getLink(linkName))
        }
    }

    @Test
    fun `setLink() and addLink() should work correctly with PersistentEntityId`() {
        val linkName = "link"
        orientDb.provider.acquireSession().use { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        orientDb.withTxSession {
            val legacyId = PersistentEntityId(issueB.id.typeId, issueB.id.localId)
            issueA.setLink(linkName, legacyId)
        }
        orientDb.withSession {
            Assert.assertEquals(issueB, issueA.getLink(linkName))
        }
        orientDb.withTxSession {
            val legacyId = PersistentEntityId(issueC.id.typeId, issueC.id.localId)
            issueB.addLink(linkName, legacyId)
        }
        orientDb.withSession {
            Assert.assertEquals(issueB, issueA.getLink(linkName))
        }
    }

    @Test
    fun `setLink() and addLink() return false if the target entity is not found`() {
        val linkName = "link"
        orientDb.provider.acquireSession().use { session ->
            session.createEdgeClass(OVertexEntity.edgeClassName(linkName))
        }

        val issueB = orientDb.createIssue("A")

        orientDb.withTxSession { tx ->
            tx.delete(issueB.id.asOId())
        }

        orientDb.withTxSession {
            assertFalse(issueB.addLink(linkName, issueB.id))
            assertFalse(issueB.addLink(linkName, ORIDEntityId.EMPTY_ID))
            assertFalse(issueB.addLink(linkName, PersistentEntityId.EMPTY_ID))
            assertFalse(issueB.addLink(linkName, PersistentEntityId(300, 300)))
        }

        orientDb.withTxSession {
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
        orientDb.withTxSession {
            issue.setProperty(propertyName, propertyValue)
        }
        orientDb.withTxSession {
            val value = issue.getProperty(propertyName)
            Assert.assertEquals(propertyValue, value)
        }
    }

    @Test
    fun `should delete property`() {
        val issue = orientDb.createIssue("DeletePropertyTest")

        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withTxSession {
            issue.setProperty(propertyName, propertyValue)
        }
        orientDb.withTxSession {
            issue.deleteProperty(propertyName)
            val value = issue.getProperty(propertyName)
            Assert.assertNull(value)
        }
    }

    @Test
    fun `set the same string blob should return false`() {
        val issue = orientDb.createIssue("GetPropertyTest")

        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withSession {
            issue.setBlobString(propertyName, propertyValue)
        }
        orientDb.withSession {
            Assert.assertEquals(false, issue.setBlobString(propertyName, propertyValue))
        }
    }

    @Test
    fun `should work with properties`() {
        val issue = orientDb.createIssue("Test1")

        orientDb.withTxSession {
            issue.setProperty("hello", "world")
            issue.setProperty("june", 6)
            issue.setProperty("year", 44L)
        }

        orientDb.withSession {
            Assert.assertEquals("world", issue.getProperty("hello"))
            Assert.assertEquals(6, issue.getProperty("june"))
            Assert.assertEquals(44L, issue.getProperty("year"))
        }

        orientDb.withTxSession {
            Assert.assertEquals(false, issue.setProperty("hello", "world"))
            Assert.assertEquals(false, issue.setProperty("june", 6))
            Assert.assertEquals(false, issue.setProperty("year", 44L))
        }

        orientDb.withSession {
            Assert.assertEquals(listOf(OVertexEntity.LOCAL_ENTITY_ID_PROPERTY_NAME, "hello", "name", "june", "year").sorted(), issue.propertyNames.sorted())
        }
    }

    @Test
    fun `should get blob size`() {
        val issue = orientDb.createIssue("BlobSizeTest")

        val blobName = "SampleBlob"
        val blobData = byteArrayOf(0x01, 0x02, 0x03)
        orientDb.withTxSession {
            issue.setBlob(blobName, ByteArrayInputStream(blobData))
        }
        orientDb.withTxSession {
            val size = issue.getBlobSize(blobName)
            Assert.assertEquals(blobData.size.toLong(), size)
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
