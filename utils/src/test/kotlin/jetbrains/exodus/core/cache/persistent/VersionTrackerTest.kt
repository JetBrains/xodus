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

    private class TestClient(val tracker: VersionTracker, val version: Version) : PersistentCacheClient {
        override fun unregister() {
            tracker.unregister(this, version)
        }

    }

    @Test
    fun `should register clients`() {
        // Given
        val versionTracker = VersionTracker()
        val client1 = TestClient(versionTracker, 1)
        val client2 = TestClient(versionTracker, 2)

        versionTracker.register(client1, 1)
        versionTracker.register(client2, 2)

        // Then
        assertEquals(false, versionTracker.hasNoClients(1))
        assertEquals(false, versionTracker.hasNoClients(2))
        assertEquals(true, versionTracker.hasNoClients(3))
    }

    @Test
    fun `should unregister clients`() {
        // Given
        val versionTracker = VersionTracker()
        val client1 = TestClient(versionTracker, 1)
        val client2 = TestClient(versionTracker, 2)

        versionTracker.register(client1, 1)
        versionTracker.register(client2, 2)

        client1.unregister()
        client2.unregister()

        // Then
        assertEquals(true, versionTracker.hasNoClients(1))
        assertEquals(true, versionTracker.hasNoClients(2))
    }
}