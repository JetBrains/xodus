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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

// Thread-safe class for tracking current version and clients registered for different versions
internal class VersionTracker(initialVersion: Long = 0) {

    internal data class ClientVersion(val client: PersistentCacheClient, val version: Long)

    private val versionRef = AtomicLong(initialVersion)

    private val registeredClients = ConcurrentHashMap.newKeySet<ClientVersion>()
    private val versionClientCount = ConcurrentHashMap<Long, Long>()

    fun nextVersion(): Long {
        return versionRef.incrementAndGet()
    }

    fun register(client: PersistentCacheClient, version: Long) {
        val wasAdded = registeredClients.add(ClientVersion(client, version))
        if (wasAdded) {
            versionClientCount.compute(version) { _, count -> if (count == null) 1 else count + 1 }
        }
    }

    fun unregister(client: PersistentCacheClient, version: Long): Long {
        val wasRemoved = registeredClients.remove(ClientVersion(client, version))
        val clientsLeft = if (wasRemoved) {
            versionClientCount.compute(version) { _, count ->
                if (count == null || (count - 1) == 0L) null else count - 1
            }
        } else {
            versionClientCount[version]
        }
        return clientsLeft ?: 0
    }

    fun hasNoClients(version: Version): Boolean {
        return !versionClientCount.containsKey(version)
    }
}