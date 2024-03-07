package jetbrains.exodus.query

import com.google.common.truth.Truth.assertThat
import io.mockk.every
import io.mockk.mockk
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.createIssue
import jetbrains.exodus.query.metadata.ModelMetaData
import org.junit.Rule
import org.junit.Test

class OrientDBQueryTest {

    @Rule
    @JvmField
    val orientDB = InMemoryOrientDB()

    @Test
    fun `should query property equal`() {
        // Given
        orientDB.createIssue("issue1")
        orientDB.createIssue("issue2")

        val model = mockk<ModelMetaData>(relaxed = true)
        val store = mockk<PersistentEntityStoreImpl>(relaxed = true)
        every { store.getAndCheckCurrentTransaction() } returns PersistentStoreTransaction(store)
        val engine = QueryEngine(model, store)

        // When
        orientDB.withSession {
            val node = PropertyEqual("name", "issue2")
            val result = engine.query("Issue", node)

            // Then
            assertThat(result.count()).isEqualTo(1)
            assertThat(result.first().getProperty("name")).isEqualTo("issue2")
        }

    }
}