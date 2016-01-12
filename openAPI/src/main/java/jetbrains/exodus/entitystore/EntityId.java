/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Each entity id consists of two parts: id of the entity type and local id within entity type.
 * So the number of entity types is bounded by the value of 2^31, and the number of entities of
 * arbitrary entity type is bounded by the value of 2^63.
 */
public interface EntityId extends Comparable<EntityId>, Serializable {

    int getTypeId();

    long getLocalId();

    @NotNull
    String toString();
}
