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

import jetbrains.exodus.util.StringInterner;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"unchecked"})
final class StoreNamingRules {

    @NonNls
    private static final String ENTITIES_SEQUENCES_PREFIX = "entities.sequences";
    @NonNls
    private static final String ENTITY_TYPES_SEQUENCE = "entity.types.sequence";
    @NonNls
    private static final String PROPERTY_IDS_SEQUENCE = "property.types.sequence"; // legacy name "types"
    @NonNls
    private static final String LINK_IDS_SEQUENCE = "link.types.sequence"; // legacy name "types"
    @NonNls
    private static final String PROPERTY_CUSTOM_TYPES_SEQUENCE = "property.custom.types.sequence";
    @NonNls
    private static final String ENTITY_TYPES_TABLE = "entity.types";
    @NonNls
    private static final String PROPERTY_IDS_TABLE = "property.types"; // legacy name "types"
    @NonNls
    private static final String LINK_IDS_TABLE = "link.types"; // legacy name "types"
    @NonNls
    private static final String PROPERTY_CUSTOM_TYPES_TABLE = "property.custom.types";
    @NonNls
    private static final String ENTITIES_TABLE_PREFIX = "entities";
    @NonNls
    private static final String PROPERTIES_TABLE_PREFIX = "properties";
    @NonNls
    private static final String LINKS_TABLE_PREFIX = "links";
    @NonNls
    private static final String BLOBS_TABLE_PREFIX = "blobs";
    @NonNls
    private static final String INTERNAL_SETTINGS = "----internal.settings----";

    @NotNull
    private final String storeName;

    StoreNamingRules(@NotNull final String storeName) {
        this.storeName = storeName;
    }

    @NotNull
    String getEntityTypesSequenceName() {
        return getFQName(ENTITY_TYPES_SEQUENCE);
    }

    @NotNull
    String getPropertyIdsSequenceName() {
        return getFQName(PROPERTY_IDS_SEQUENCE);
    }

    @NotNull
    String getLinkIdsSequenceName() {
        return getFQName(LINK_IDS_SEQUENCE);
    }

    @NotNull
    String getPropertyCustomTypesSequence() {
        return getFQName(PROPERTY_CUSTOM_TYPES_SEQUENCE);
    }

    @NotNull
    String getEntitiesSequenceName(final int entityTypeId) {
        return getFQName(ENTITIES_SEQUENCES_PREFIX, entityTypeId);
    }

    @NotNull
    String getEntityTypesTableName() {
        return getFQName(ENTITY_TYPES_TABLE);
    }

    @NotNull
    String getPropertyIdsTableName() {
        return getFQName(PROPERTY_IDS_TABLE);
    }

    @NotNull
    String getLinkIdsTableName() {
        return getFQName(LINK_IDS_TABLE);
    }

    @NotNull
    String getPropertyCustomTypesTable() {
        return getFQName(PROPERTY_CUSTOM_TYPES_TABLE);
    }

    @NotNull
    String getEntitiesTableName(final int entityTypeId) {
        return getFQName(ENTITIES_TABLE_PREFIX, entityTypeId);
    }

    @NotNull
    String getPropertiesTableName(final int entityTypeId) {
        return getFQName(PROPERTIES_TABLE_PREFIX, entityTypeId);
    }

    @NotNull
    String getLinksTableName(final int entityTypeId) {
        return getFQName(LINKS_TABLE_PREFIX, entityTypeId);
    }

    @NotNull
    public String getBlobsTableName(final int entityTypeId) {
        return getFQName(BLOBS_TABLE_PREFIX, entityTypeId);
    }

    @NotNull
    public String getInternalSettingsName() {
        return getFQName(INTERNAL_SETTINGS);
    }

    /**
     * Gets fully-qualified name of a table or sequence.
     *
     * @param localName local table name.
     * @param params    params.
     * @return fully-qualified table name.
     */
    @NotNull
    private String getFQName(@NotNull final String localName, Object... params) {
        final StringBuilder builder = new StringBuilder();
        builder.append(storeName);
        builder.append('.');
        builder.append(localName);
        for (final Object param : params) {
            builder.append('#');
            builder.append(param);
        }
        //noinspection ConstantConditions
        return StringInterner.intern(builder.toString());
    }
}
