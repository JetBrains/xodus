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
package jetbrains.exodus.core.cache.persistent

import org.junit.Assert.assertEquals
import org.junit.Test


class VersionTrackerTest {

    @Test
    fun `should register clients`() {
        // Given
        val versionTracker = VersionTracker()

        versionTracker.incrementClients(1)
        versionTracker.incrementClients(2)

        // Then
        assertEquals(false, versionTracker.hasNoClients(1))
        assertEquals(false, versionTracker.hasNoClients(2))
        assertEquals(true, versionTracker.hasNoClients(3))
    }

    @Test
    fun `should unregister clients`() {
        // Given
        val versionTracker = VersionTracker()

        versionTracker.incrementClients(1)
        versionTracker.incrementClients(2)

        versionTracker.decrementClients(1)
        versionTracker.decrementClients(2)

        // Then
        assertEquals(true, versionTracker.hasNoClients(1))
        assertEquals(true, versionTracker.hasNoClients(2))
    }
}