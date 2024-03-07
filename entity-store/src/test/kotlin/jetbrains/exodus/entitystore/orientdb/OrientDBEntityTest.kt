package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream

class OrientDBEntityTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

    @Test
    fun `multiple links should work`() {
        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")
        val linkName = "link"
        orientDb.withSessionNoTx { session ->
            session.createEdgeClass(linkName)
        }

        val (entityA, entityB, entityC) = orientDb.withSession {
            Triple(OrientDBEntity(issueA), OrientDBEntity(issueB), OrientDBEntity(issueC))
        }

        orientDb.withSession {
            entityA.addLink(linkName, entityB)
            entityA.addLink(linkName, entityC)
        }

        orientDb.withSession {
            val links = entityA.getLinks(linkName)
            Assert.assertTrue(links.contains(entityB))
            Assert.assertTrue(links.contains(entityC))
        }
    }

    @Test
    fun `add blob should be reflected in get blob names`() {
        val issue = orientDb.createIssue("TestBlobs")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }

        orientDb.withSession {
            entity.setBlob("blob1", ByteArrayInputStream(byteArrayOf(0x01, 0x02, 0x03)))
            entity.setBlob("blob2", ByteArrayInputStream(byteArrayOf(0x04, 0x05, 0x06)))
            entity.setBlob("blob3", ByteArrayInputStream(byteArrayOf(0x07, 0x08, 0x09)))
            entity.setProperty("version", 99)
        }

        orientDb.withSession {
            val blobNames = entity.getBlobNames()
            Assert.assertTrue(blobNames.contains("blob1"))
            Assert.assertTrue(blobNames.contains("blob2"))
            Assert.assertTrue(blobNames.contains("blob3"))
            Assert.assertEquals(3, blobNames.size)
        }
    }

    @Test
    fun `should delete all links`() {
        val linkName = "link"
        orientDb.withSessionNoTx { session ->
            session.createEdgeClass(linkName)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        val (entityA, entB, entC) = orientDb.withSession {
            Triple(OrientDBEntity(issueA), OrientDBEntity(issueB), OrientDBEntity(issueC))
        }

        orientDb.withSession {
            entityA.addLink(linkName, entB)
            entityA.addLink(linkName, entC)
        }

        orientDb.withSession {
            entityA.deleteLinks(linkName)
            val links = entityA.getLinks(linkName)
            Assert.assertEquals(0, links.size())
        }
    }

    @Test
    fun `should delete element holding blob after blob deleted from entity`() {
        val issue = orientDb.createIssue("TestBlobDelete")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }

        val linkName = "blobForDeletion"
        orientDb.withSession {
            entity.setBlob(linkName, ByteArrayInputStream(byteArrayOf(0x11, 0x12, 0x13)))
        }
        val blob = orientDb.withSessionNoTx {
            issue.reload<OVertex>()
            issue.getLinkProperty(linkName)!!
        }

        orientDb.withSession {
            Assert.assertNotNull(it.load<OElement>(blob.identity))
            entity.deleteBlob(linkName)
        }

        orientDb.withSession {
            val blobNames = entity.getBlobNames()
            Assert.assertFalse(blobNames.contains(linkName))
            Assert.assertEquals(null, it.load<OElement>(blob.identity))
        }
    }

    @Test
    fun `should replace a link correctly`() {
        val linkName = "link"
        orientDb.withSessionNoTx { session ->
            session.createEdgeClass(linkName)
        }

        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")

        val (entityA, entB, entC) = orientDb.withSession {
            Triple(OrientDBEntity(issueA), OrientDBEntity(issueB), OrientDBEntity(issueC))
        }

        orientDb.withSession {
            entityA.setLink(linkName, entB.id)
        }
        orientDb.withSessionNoTx {
            Assert.assertEquals(entB, entityA.getLink(linkName))
        }
        orientDb.withSession {
            entityA.setLink(linkName, entC.id)
        }
        orientDb.withSessionNoTx {
            Assert.assertEquals(entC, entityA.getLink(linkName))
        }

    }


    @Test
    fun `should get property`() {
        val issue = orientDb.createIssue("GetPropertyTest")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }
        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withSession {
            entity.setProperty(propertyName, propertyValue)
        }
        orientDb.withSession {
            val value = entity.getProperty(propertyName)
            Assert.assertEquals(propertyValue, value)
        }
    }

    @Test
    fun `should delete property`() {
        val issue = orientDb.createIssue("DeletePropertyTest")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }
        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withSession {
            entity.setProperty(propertyName, propertyValue)
        }
        orientDb.withSession {
            entity.deleteProperty(propertyName)
            val value = entity.getProperty(propertyName)
            Assert.assertNull(value)
        }
    }


    @Test
    fun `set the same string blob should return false`() {
        val issue = orientDb.createIssue("GetPropertyTest")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }
        val propertyName = "SampleProperty"
        val propertyValue = "SampleValue"
        orientDb.withSession {
            entity.setBlobString(propertyName, propertyValue)
        }
        orientDb.withSession {
            Assert.assertEquals(false, entity.setBlobString(propertyName, propertyValue))
        }
    }


    @Test
    fun `should work with properties`() {
        val issue = orientDb.createIssue("Test1")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }

        orientDb.withSession {
            entity.setProperty("hello", "world")
            entity.setProperty("june", 6)
            entity.setProperty("year", 44L)
        }

        orientDb.withSessionNoTx {
            Assert.assertEquals("world", entity.getProperty("hello"))
            Assert.assertEquals(6, entity.getProperty("june"))
            Assert.assertEquals(44L, entity.getProperty("year"))
        }

        orientDb.withSession {
            Assert.assertEquals(false, entity.setProperty("hello", "world"))
            Assert.assertEquals(false, entity.setProperty("june", 6))
            Assert.assertEquals(false, entity.setProperty("year", 44L))
        }

        orientDb.withSessionNoTx {
            Assert.assertEquals(listOf("hello", "name", "june", "year").sorted(), entity.propertyNames.sorted())
        }
    }


    @Test
    fun `should get blob size`() {
        val issue = orientDb.createIssue("BlobSizeTest")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }
        val blobName = "SampleBlob"
        val blobData = byteArrayOf(0x01, 0x02, 0x03)
        orientDb.withSession {
            entity.setBlob(blobName, ByteArrayInputStream(blobData))
        }
        orientDb.withSession {
            val size = entity.getBlobSize(blobName)
            Assert.assertEquals(blobData.size.toLong(), size)
        }
    }



}
