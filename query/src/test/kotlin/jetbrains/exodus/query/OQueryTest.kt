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

class OQueryTest {

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
            val result = engine.query("Issue", node).toList()

            // Then
            assertThat(result.count()).isEqualTo(1)
            assertThat(result.first().getProperty("name")).isEqualTo("issue2")
        }

    }

    @Test
    fun `should query with or`() {
        // Given
        orientDB.createIssue("issue1")
        orientDB.createIssue("issue2")
        orientDB.createIssue("issue3")

        val model = mockk<ModelMetaData>(relaxed = true)
        val store = mockk<PersistentEntityStoreImpl>(relaxed = true)
        every { store.getAndCheckCurrentTransaction() } returns PersistentStoreTransaction(store)
        val engine = QueryEngine(model, store)

        // When
        orientDB.withSession {
            val equal1 = PropertyEqual("name", "issue1")
            val equal3 = PropertyEqual("name", "issue3")
            val result = engine.query("Issue", Or(equal1, equal3)).instantiate()

            // Then
            assertThat(result.count()).isEqualTo(2)
            assertThat(result.first().getProperty("name")).isEqualTo("issue1")
            assertThat(result.last().getProperty("name")).isEqualTo("issue3")
        }
    }

    @Test
    fun `should query with and`() {
        // Given
        orientDB.createIssue("issue1", "project1")
        orientDB.createIssue("issue2", "project1")
        orientDB.createIssue("issue3", "project2")

        val model = mockk<ModelMetaData>(relaxed = true)
        val store = mockk<PersistentEntityStoreImpl>(relaxed = true)
        every { store.getAndCheckCurrentTransaction() } returns PersistentStoreTransaction(store)
        val engine = QueryEngine(model, store)

        // When
        orientDB.withSession {
            val nameEqual = PropertyEqual("name", "issue2")
            val projectEqual = PropertyEqual("project", "project1")
            val result = engine.query("Issue", And(nameEqual, projectEqual)).instantiate()

            // Then
            assertThat(result.count()).isEqualTo(1)
            assertThat(result.first().getProperty("name")).isEqualTo("issue2")
            assertThat(result.first().getProperty("project")).isEqualTo("project1")
        }
    }
}