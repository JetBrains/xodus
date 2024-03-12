package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
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

        val newClassName = "Other${IssueClass.NAME}"
        store.renameEntityType(IssueClass.NAME, newClassName)
        val issueByNewName =  store.computeInExclusiveTransaction{
            it as OStoreTransaction
            (it.activeSession() as ODatabaseSession).queryEntity("select from $newClassName").firstOrNull()
        }
        Assert.assertNotNull(issueByNewName)
        issueByNewName!!
        Assert.assertEquals(summary, issueByNewName.getProperty("name"))
    }
}
