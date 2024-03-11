package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream

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
            session.createEdgeClass(linkName)
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
    fun `should delete all links`() {
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(linkName)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        val (entityA, entB, entC) = orientDb.withTxSession {
            Triple(issueA, issueB, issueC)
        }

        orientDb.withTxSession {
            entityA.addLink(linkName, entB)
            entityA.addLink(linkName, entC)
        }

        orientDb.withTxSession {
            entityA.deleteLinks(linkName)
            val links = entityA.getLinks(linkName)
            Assert.assertEquals(0, links.size())
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
            issue.asVertex.reload<OVertex>()
            issue.asVertex.getLinkProperty(linkName)!!
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
        orientDb.withSession { session ->
            session.createEdgeClass(linkName)
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
        orientDb.withTxSession {
            issue.setBlobString(propertyName, propertyValue)
        }
        orientDb.withTxSession {
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
            Assert.assertEquals(listOf("hello", "name", "june", "year").sorted(), issue.propertyNames.sorted())
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
}
