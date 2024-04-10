package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.Issues
import jetbrains.exodus.entitystore.orientdb.testutil.createIssue
import org.junit.Assert
import org.junit.Rule
import org.junit.Test

class DBCompactTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

    @Test
    fun `database compacter should work`() {
        (0..99).forEach {
            orientDb.createIssue("Test issue ${it}")
        }
        orientDb.provider.compact()
        orientDb.store.executeInTransaction {
            val size = it.getAll(Issues.CLASS).size()
            Assert.assertEquals(100, size)
        }
    }
}
