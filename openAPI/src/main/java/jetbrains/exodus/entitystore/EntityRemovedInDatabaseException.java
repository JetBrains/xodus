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
package jetbrains.exodus.entitystore;

/**
 * {@code EntityRemovedInDatabaseException} is thrown by {@linkplain StoreTransaction#getEntity(EntityId)} on
 * attempt to get {@linkplain Entity} by an {@linkplain EntityId} which wss removed from the database.
 *
 * @see StoreTransaction#getEntity(EntityId)
 */
public class EntityRemovedInDatabaseException extends EntityStoreException {

    /**
     * @param entityType type of an entity that was not found in database
     */
    public EntityRemovedInDatabaseException(final String entityType) {
        super(entityType + " was removed.");
    }

}
