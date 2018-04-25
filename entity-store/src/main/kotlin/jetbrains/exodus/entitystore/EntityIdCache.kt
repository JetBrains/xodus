/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore

import jetbrains.exodus.core.dataStructures.ConcurrentObjectCache
import java.util.regex.Pattern

internal object EntityIdCache {

    private val ID_SPLIT_PATTERN = Pattern.compile("-")
    private val theCache = ConcurrentObjectCache<CharSequence, PersistentEntityId>(
            Integer.getInteger("jetbrains.exodus.entitystore.entityIdCacheSize", 997 * 3))

    @JvmStatic
    fun getEntityId(representation: CharSequence): PersistentEntityId {
        var result = theCache.tryKey(representation)
        if (result == null) {
            val idParts = ID_SPLIT_PATTERN.split(representation)
            val partsCount = idParts.size
            if (partsCount != 2) {
                throw IllegalArgumentException("Invalid structure of entity id")
            }
            val entityTypeId = Integer.parseInt(idParts[0])
            val entityLocalId = java.lang.Long.parseLong(idParts[1])
            result = PersistentEntityId(entityTypeId, entityLocalId)
            theCache.cacheObject(representation, result)
        }
        return result
    }
}