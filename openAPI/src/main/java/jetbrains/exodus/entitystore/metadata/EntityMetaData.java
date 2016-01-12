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

import jetbrains.exodus.entitystore.Entity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface EntityMetaData {

    ModelMetaData getModelMetaData();

    @NotNull
    String getType();

    @Nullable
    String getSuperType();

    Collection<String> getInterfaceTypes();

    Iterable<String> getThisAndSuperTypes();

    boolean hasSubTypes();

    Collection<String> getSubTypes();

    Collection<String> getAllSubTypes();

    AssociationEndMetaData getAssociationEndMetaData(@NotNull String name);

    @NotNull
    Collection<AssociationEndMetaData> getAssociationEndsMetaData();

    @NotNull
    Iterable<PropertyMetaData> getPropertiesMetaData();

    PropertyMetaData getPropertyMetaData(String name);

    /**
     * Own indexes only
     *
     * @return
     */
    @NotNull
    Set<Index> getOwnIndexes();

    /**
     * Indexes, including inheritors
     *
     * @return
     */
    @NotNull
    Set<Index> getIndexes();

    /**
     * Indexes for given field, including inheritors
     *
     * @return
     */
    @NotNull
    Set<Index> getIndexes(String field);

    @NotNull
    Set<String> getRequiredProperties();

    @NotNull
    Set<String> getRequiredIfProperties(Entity e);

    /**
     * VersionMismatch resolution for whole class
     *
     * @return
     */
    boolean isVersionMismatchIgnoredForWholeClass();

    /**
     * VersionMismatch resolution for concrete property
     *
     * @param propertyName
     * @return
     */
    @Deprecated
    boolean isVersionMismatchIgnored(String propertyName);

    boolean isHistoryIgnored(String propertyName);

    @Nullable
    Runnable getInitializer();

    boolean getRemoveOrphan();

    boolean hasAggregationChildEnds();

    Set<String> getAggregationChildEnds();

    @NotNull
    Map<String, Set<String>> getIncomingAssociations(ModelMetaData mmd);

}
