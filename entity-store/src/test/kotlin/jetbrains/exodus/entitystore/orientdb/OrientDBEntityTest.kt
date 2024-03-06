package jetbrains.exodus.entitystore.orientdb

import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayInputStream

class OrientDBEntityTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

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

        orientDb.withSession {
            Assert.assertEquals("world", entity.getProperty("hello"))
            Assert.assertEquals(6, entity.getProperty("june"))
            Assert.assertEquals(44L, entity.getProperty("year"))
        }
    }

    @Test
    fun `should return false when setting same blob string`() {
        val issue = orientDb.createIssue("Test2")
        val entity = orientDb.withSession {
            OrientDBEntity(issue)
        }

        orientDb.withSession {
            entity.setBlobString("blobString", "blobValue")
        }

        orientDb.withSession {
            Assert.assertFalse(entity.setBlobString("blobString", "blobValue"))
        }
    }

    @Test
    fun `should link issues A, B and C`() {
        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")
        val linkName = "link"
        orientDb.withSession { session->
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
        val issueA = orientDb.createIssue("A")
        val issueB = orientDb.createIssue("B")
        val issueC = orientDb.createIssue("C")
        val linkName = "link"
        orientDb.withSession { session ->
            session.createEdgeClass(linkName)
        }

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
}
