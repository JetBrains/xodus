package jetbrains.exodus.query.metadata


import org.junit.Test
import kotlin.test.assertFailsWith

class SortingTest {

    @Test
    fun `if two entities have the same type, throw exception`() {
        val entities = listOf(
            entity("type1"),
            entity("type1")
        )

        assertFailsWith<IllegalArgumentException> {
            entities.sortedTopologically()
        }
    }

    @Test
    fun `if graph contains cycles, throw exception`() {
        val entities = listOf(
            entity("type1", superType = "type3"),
            entity("type2", superType = "type1"),
            entity("type3", superType = "type2")
        )

        assertFailsWith<IllegalArgumentException> {
            entities.sortedTopologically()
        }
    }

    @Test
    fun `if superClass is not among entities, throw exception`() {
        val entities = listOf(
            entity("type1", superType = "type2")
        )

        assertFailsWith<IllegalArgumentException> {
            entities.sortedTopologically()
        }
    }

    @Test
    fun sort() {
        val entities = listOf(
            entity("type1"),
            entity("type2", superType = "type1"),
            entity("type3", superType = "type2"),
            entity("type4", superType = "type1"),
            entity("type5", superType = "type2"),
        ).shuffled()

        val sorted = entities.sortedTopologically().map { it.type }
        for (entity in entities) {
            if (entity.superType != null) {
                sorted.checkBefore(entity.superType!!, entity.type)
            }
        }
    }

    private fun List<String>.checkBefore(before: String, after: String) {
        require(indexOf(before) < indexOf(after))
    }

    private fun entity(type: String, superType: String? = null): EntityMetaData {
        val res = EntityMetaDataImpl()
        res.type = type
        res.superType = superType
        return res
    }
}