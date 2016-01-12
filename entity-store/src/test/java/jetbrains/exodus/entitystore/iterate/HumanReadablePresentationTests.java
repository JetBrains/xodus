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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.iterate.binop.*;

import java.util.ArrayList;
import java.util.Comparator;

public class HumanReadablePresentationTests extends EntityStoreTestBase {

    @SuppressWarnings({"HardcodedLineSeparator", "OverlyCoupledMethod"})
    public void test1() {
        assertEquals(EntityIterableType.values().length, EntityIterableBase.children.length);
        assertEquals(EntityIterableType.values().length, EntityIterableBase.fields.length);
        final PersistentStoreTransaction txn = getStoreTransaction();
        assertNotNull(txn);
        final PersistentEntityStoreImpl store = getEntityStore();
        checkIterable(new AddNullDecoratorIterable(store,
                        new PropertyRangeIterable(store, 0, 1, "minValue", "maxValue"), EntityIterableBase.EMPTY),
                "Left operand appended with null if it's present in the right, but not left one\n" +
                        "|   Entities with a property value in range 0 1 minvalue maxvalue\n" +
                        "|   Empty iterable"
        );
        checkIterable(new UnionIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Union\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new MinusIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Minus\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new IntersectionIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Intersection\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new ConcatenationIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Concatenation\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new PropertiesIterable(store, 0, 1), "Entities with property sorted by its value 0 1");
        checkIterable(new PropertyValueIterable(store, 0, 1, "value"), "Entities with specified property value 0 1 value");
        checkIterable(new EntitiesOfTypeIterable(txn, store, 0), "All entities of specific type 0");
        checkIterable(new EntitiesOfTypeRangeIterable(txn, store, 0, 3, 8), "Entities of specific type within id range 0 3 8");
        checkIterable(new EntitiesWithLinkIterable(store, 0, 1), "Entities with link 0 1");
        checkIterable(new EntitiesWithLinkSortedIterable(store, 0, 1, 2, 3), "Entities with link 0 1");
        checkIterable(new SingleEntityIterable(store, new PersistentEntityId(0, 1)), "Single entity 0 1");
        //noinspection ComparatorMethodParameterNotUsed
        checkIterable(new MergeSortedIterable(store, new ArrayList<EntityIterable>(), new Comparator<Entity>() {
            @Override
            public int compare(Entity o1, Entity o2) {
                return 0;
            }
        }), "Merge sorted iterables 0");
        checkIterable(new EntitiesWithPropertyIterable(store, 0, 1), "Entities with property 0 1");
        checkIterable(new EntitiesWithBlobIterable(store, 0, 1), "Entities with blob 0 1");
        checkIterable(new TakeEntityIterable(store, EntityIterableBase.EMPTY, 0),
                "Take iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new EntityReverseIterable(store, EntityIterableBase.EMPTY),
                "Reversed iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SortIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY, 0, false),
                "Sorting iterable 0 1\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new DistinctIterable(store, EntityIterableBase.EMPTY),
                "Distinct iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SelectManyDistinctIterable(store, EntityIterableBase.EMPTY, 0),
                "SelectMany distinct iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SortResultIterable(store, EntityIterableBase.EMPTY), "Empty iterable");
        checkIterable(new SelectDistinctIterable(store, EntityIterableBase.EMPTY, 0),
                "Select distinct iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SkipEntityIterable(store, EntityIterableBase.EMPTY, 0),
                "Skip iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new ExcludeNullIterableDecorator(store, EntityIterableBase.EMPTY),
                "Exclude null\n" +
                        "|   Empty iterable"
        );
        IntHashMap<String> map = new IntHashMap<>();
        map.put(3, "value3");
        map.put(4, "value2");
        checkIterable(new EntityFromLinkSetIterable(txn, store, new PersistentEntityId(0, 1), map),
                "Outgoing links of a set from an entity 0 1  2 links: 3 4");
        checkIterable(new EntityFromLinksIterable(txn, store, new PersistentEntityId(0, 1), 2),
                "Outgoing links from an entity 0 1 2");
        checkIterable(new EntityToLinksIterable(store, new PersistentEntityId(0, 1), 2, 3),
                "Incoming links to an entity 0 1 2 3");
        checkIterable(new FilterEntityTypeIterable(store, 0, EntityIterableBase.EMPTY),
                "Filter source iterable by entity type 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new FilterLinksIterable(store, 0, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Filter source iterable by links set 0\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        // XD-441
        checkIterable(new IntersectionIterable(store,
                        new IntersectionIterable(store,
                                new IntersectionIterable(store,
                                        new UnionIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY)),
                                new IntersectionIterable(store,
                                        new UnionIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY))),
                        new IntersectionIterable(store,
                                new IntersectionIterable(store,
                                        new UnionIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY)),
                                new IntersectionIterable(store,
                                        new UnionIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(store, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY)))),
                "Intersection\n" +
                        "|   Intersection\n" +
                        "|   |   Intersection\n" +
                        "|   |   |   Union\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   Minus\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   Intersection\n" +
                        "|   |   |   Union\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   Minus\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   Intersection\n" +
                        "|   |   Intersection\n" +
                        "|   |   |   Union\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   Minus\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   Intersection\n" +
                        "|   |   |   Union\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   Minus\n" +
                        "|   |   |   |   Empty iterable\n" +
                        "|   |   |   |   Empty iterable");
    }

    private void checkIterable(EntityIterableBase iterable, String presentation) {
        assertEquals(presentation, EntityIterableBase.getHumanReadablePresentation(iterable.getHandle()));
        EntityIterableBase instantiated = EntityIterableBase.instantiate(getStoreTransaction(), getEntityStore(), presentation);
        assertEquals(presentation, EntityIterableBase.getHumanReadablePresentation(instantiated.getHandle()));
    }
}
