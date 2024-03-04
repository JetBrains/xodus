/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class PersistentEntityStoreCacheConfigTest {

    @Test
    fun `should return default max cache weight`() {
        // Given
        val config = PersistentEntityStoreConfig()

        // When
        val maxCacheWeight = config.entityIterableCacheWeight

        // Then
        val maxMemory = Runtime.getRuntime().maxMemory()
        assertTrue(maxCacheWeight in 1..<maxMemory)
    }

    @Test
    fun `should calculate max cache weight from params`() {
        // Given
        System.setProperty(PersistentEntityStoreConfig.ENTITY_ITERABLE_CACHE_MEMORY_PERCENTAGE, "50")
        System.setProperty(PersistentEntityStoreConfig.ENTITY_ITERABLE_CACHE_ENTITY_WEIGHT, "8")
        val config = PersistentEntityStoreConfig()

        // When
        val maxCacheWeight = config.entityIterableCacheWeight

        // Then
        val maxMemory = Runtime.getRuntime().maxMemory()
        assertEquals(maxCacheWeight, (maxMemory * 50) / (100 * 8))
    }
}