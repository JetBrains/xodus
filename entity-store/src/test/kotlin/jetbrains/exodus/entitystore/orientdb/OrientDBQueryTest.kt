package jetbrains.exodus.entitystore.orientdb

import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test

class OrientDBQueryTest {

    @Rule
    @JvmField
    val orientDb = InMemoryOrientDB()

    @Test
    fun `should convert query result to iterable entity`() {
        // Given
        orientDb.createIssue("Test1")
        orientDb.createIssue("Test2")

        // When
        orientDb.withTxSession {

            // Then
            val iterable = it.queryEntity("select from Issue")
            assertEquals(2, iterable.count())
            assertEquals(listOf("Test1", "Test2"), iterable.map { it.getProperty(IssueClass.NAME_PROPERTY) })
        }
    }
}