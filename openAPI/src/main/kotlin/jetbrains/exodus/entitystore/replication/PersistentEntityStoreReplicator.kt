/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.replication

import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.entitystore.BlobVault
import jetbrains.exodus.entitystore.DiskBasedBlobVault
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.replication.EnvironmentReplicationDelta

interface PersistentEntityStoreReplicator {
    fun beginReplication(environment: Environment): EnvironmentReplicationDelta

    fun replicateEnvironment(delta: EnvironmentReplicationDelta, environment: Environment)

    fun replicateBlobVault(delta: EnvironmentReplicationDelta, vault: BlobVault, blobsToReplicate: List<Pair<Long, Long>>)

    fun decorateBlobVault(vault: DiskBasedBlobVault, store: PersistentEntityStore): DiskBasedBlobVault

    fun endReplication(delta: EnvironmentReplicationDelta)
}
