package jetbrains.exodus.entitystore

import io.mockk.every
import io.mockk.mockk
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class EntityIterableCacheReverseIndexTest {

    @Test
    fun `should distribute store handles`() {
        // Given
        val index = EntityIterableCacheReverseIndex()
        val handle = mockk<EntityIterableHandle> {
            every { linkIds } returns intArrayOf(1)
            every { propertyIds } returns intArrayOf(1)
            every { typeIdsAffectingCreation } returns intArrayOf(1)
            every { entityTypeId } returns 1
        }
        index.add(handle)

        // When
        val linkIdHandles = index.getLinkIdHandles(1)!!.toList()
        val propertyIdHandles = index.getPropertyIdHandles(1)!!.toList()
        val typeIdAffectingCreationHandles = index.getTypeIdAffectingCreationHandles(1)!!.toList()
        val entityTypeIdHandles = index.getEntityTypeIdHandles(1)!!.toList()

        // Then
        assertTrue(linkIdHandles.contains(handle) == true)
        assertTrue(linkIdHandles.size == 1)

        assertTrue(propertyIdHandles.contains(handle) == true)
        assertTrue(propertyIdHandles.size == 1)

        assertTrue(typeIdAffectingCreationHandles.contains(handle) == true)
        assertTrue(typeIdAffectingCreationHandles.size == 1)

        assertTrue(entityTypeIdHandles.contains(handle) == true)
        assertTrue(entityTypeIdHandles.size == 1)

    }

    @Test
    fun `should be persistent`() {
        // Given
        val origin = EntityIterableCacheReverseIndex()

        val links = intArrayOf(1)
        val handle1 = givenMockHandle(links)
        val handle2 = givenMockHandle(links)
        origin.add(handle1)
        origin.add(handle2)
        // Initial mapping: 1 -> [handle1, handle2]

        // When
        val clone = origin.clone()

        val handle3 = givenMockHandle(links)
        clone.add(handle3)
        clone.remove(handle1)
        // Final mapping: 1 -> [handle2, handle3]

        val handle4 = givenMockHandle(links)
        origin.add(handle4)
        origin.remove(handle2)
        // Final mapping: 1 -> [handle1, handle4]

        // Then
        assertTrue(clone != origin)
        val originHandles = origin.getLinkIdHandles(1)!!.toSet()
        val cloneHandles = clone.getLinkIdHandles(1)!!.toSet()
        assertEquals("Original index should not be modified by clone", setOf(handle1, handle4), originHandles)
        assertEquals("Clone index should not be modified by origin", setOf(handle2, handle3), cloneHandles)
    }

    private fun givenMockHandle(links: IntArray): EntityIterableHandle {
        return mockk(relaxed = true) {
            every { linkIds } returns links
        }
    }
}