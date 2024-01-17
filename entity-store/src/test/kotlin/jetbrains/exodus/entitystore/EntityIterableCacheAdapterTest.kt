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
import org.junit.Test


class EntityIterableCacheAdapterTest {

    @Test
    fun `should create weighted cache`() {
        // Given
        // Use default config values
        val config = PersistentEntityStoreConfig()
        val maxCacheWeight = config.entityIterableCacheWeight

        // When
        val adapter = EntityIterableCacheAdapter.create(config)

        // Then
        assertEquals(maxCacheWeight, adapter.cache.size())
    }

    @Test
    fun `should create sized cache for backward compatibility`() {
        // Given
        System.setProperty(PersistentEntityStoreConfig.ENTITY_ITERABLE_CACHE_SIZE, "100")
        val config = PersistentEntityStoreConfig()

        // When
        val adapter = EntityIterableCacheAdapter.create(config)

        // Then
        assertEquals(100, adapter.cache.size())
    }
}