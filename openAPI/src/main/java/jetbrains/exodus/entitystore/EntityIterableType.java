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

public enum EntityIterableType {

    EMPTY("Empty iterable", 0),
    ALL_ENTITIES("All entities of specific type", 1),
    SINGLE_ENTITY("Single entity", 2),
    ENTITY_FROM_LINKS("Outgoing links from an entity", 3),
    HISTORY_ENTITY_FROM_LINKS("Outgoing links from a history entity", 4),
    ENTITY_TO_LINKS("Incoming links to an entity", 5),
    ENTITIES_WITH_LINK("Entities with link", 6),
    ENTITIES_WITH_PROPERTY("Entities with property", 7),
    ENTITIES_WITH_PROPERTY_SORTED_BY_VALUE("Entities with property sorted by its value", 8),
    ENTITIES_BY_PROP_VALUE("Entities with specified property value", 9),
    ENTITIES_BY_PROP_VALUE_IN_RANGE("Entities with a property value in range", 10),
    ENTITIES_WITH_BLOB("Entities with blob", 11),
    INTERSECT("Intersection", 12),
    UNION("Union", 13),
    MINUS("Minus", 14),
    CONCAT("Concatenation", 15),
    REVERSE("Reversed iterable", 16),
    SORTING("Sorting iterable", 17),
    SORTING_LINKS("Sorting links iterable", 18),
    MERGE_SORTED("Merge sorted iterables", 19),
    DISTINCT("Distinct iterable", 20),
    SELECT_DISTINCT("Select distinct iterable", 21),
    SELECTMANY_DISTINCT("SelectMany distinct iterable", 22),
    SKIP("Skip iterable", 23),
    TAKE("Take iterable", 24),
    SEARCH_RESULTS("Search results iterable", 25),
    ENTITY_FROM_LINKS_SET("Outgoing links of a set from an entity", 26),
    HISTORY_ENTITY_FROM_LINKS_SET("Outgoing links of a set from a history entity", 27),
    ADD_NULL("Left operand appended with null if it's present in the right, but not left one", 28),
    EXCLUDE_NULL("Exclude null", 29),
    FILTER_ENTITY_TYPE("Filter source iterable by entity type", 30),
    FILTER_LINKS("Filter source iterable by links set", 31),
    ALL_ENTITIES_RANGE("Entities of specific type within id range", 32);

    private final String description;
    private final int type;

    EntityIterableType(final String description, final int type) {
        this.description = description;
        this.type = type;
    }

    @NotNull
    public String getDescription() {
        return description;
    }

    public int getType() {
        return type;
    }

}
