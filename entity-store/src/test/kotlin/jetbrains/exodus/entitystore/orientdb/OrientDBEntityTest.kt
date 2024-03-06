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
    fun `should link issues A, B and C`() {
        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")
        val linkName = "link"
        orientDb.withSessionNoTx { session->
            session.createEdgeClass(linkName)
        }

        val (entityA,entityB,entityC) = orientDb.withSession {
            Triple(OrientDBEntity(issueA),OrientDBEntity(issueB),OrientDBEntity(issueC))
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
    fun `should add blobs to issue and get their names`() {
        val issue = orientDb.createIssue("TestBlobs")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }

        orientDb.withSession {
            entity.setBlob("blob1",  ByteArrayInputStream(byteArrayOf(0x01, 0x02, 0x03)))
            entity.setBlob("blob2",  ByteArrayInputStream(byteArrayOf(0x04, 0x05, 0x06)))
            entity.setBlob("blob3",  ByteArrayInputStream(byteArrayOf(0x07, 0x08, 0x09)))
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
    fun `should delete all links from an entity`() {
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
    fun `should delete blob content after issue blob deletion`() {
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
    fun `should set a link correctly`() {
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













}
