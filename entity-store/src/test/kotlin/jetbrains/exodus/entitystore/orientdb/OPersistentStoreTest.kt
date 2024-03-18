package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.record.OVertex
import org.junit.Assert
import org.junit.Rule
import org.junit.Test

class OPersistentStoreTest {
    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()


    @Test
    fun renameClassTest() {
        val summary = "Hello, your product does not work"
        orientDb.createIssue(summary)
        val store = orientDb.store

        val newClassName = "Other${Issues.CLASS}"
        store.renameEntityType(Issues.CLASS, newClassName)
        val issueByNewName =  store.computeInExclusiveTransaction{
            it as OStoreTransaction
            (it.activeSession() as ODatabaseSession).queryEntities("select from $newClassName").firstOrNull()
        }
        Assert.assertNotNull(issueByNewName)
        issueByNewName!!
        Assert.assertEquals(summary, issueByNewName.getProperty("name"))
    }

    @Test
    fun transactionPropertiesTest(){
        val issue = orientDb.createIssue("Hello, nothing works")
        val store = orientDb.store
        store.computeInTransaction {
            Assert.assertTrue(it.isIdempotent)
            issue.asVertex.reload<OVertex>()
            issue.setProperty("version", "22")
            Assert.assertFalse(it.isIdempotent)
        }
    }
}
