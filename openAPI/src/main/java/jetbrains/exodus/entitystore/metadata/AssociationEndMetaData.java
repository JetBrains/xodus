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
package jetbrains.exodus.entitystore.metadata;

import org.jetbrains.annotations.NotNull;

public interface AssociationEndMetaData extends MemberMetaData {

    @NotNull
    EntityMetaData getOppositeEntityMetaData();

    /**
     * Cascade delete association target entity
     *
     * @return
     */
    boolean getCascadeDelete();

    /**
     * Remove association with target on entity delete
     *
     * @return
     */
    boolean getClearOnDelete();

    /**
     * Target cascade delete association target entity
     *
     * @return
     */
    boolean getTargetCascadeDelete();

    /**
     * Target remove association with target on entity delete
     *
     * @return
     */
    boolean getTargetClearOnDelete();

    @NotNull
    AssociationEndCardinality getCardinality();

    @NotNull
    AssociationMetaData getAssociationMetaData();

    @NotNull
    AssociationEndType getAssociationEndType();

}
