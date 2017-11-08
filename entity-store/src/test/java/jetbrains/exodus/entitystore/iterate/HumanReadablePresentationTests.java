/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
        checkIterable(new AddNullDecoratorIterable(txn,
                        new PropertyRangeIterable(txn, 0, 1, "minValue", "maxValue"), EntityIterableBase.EMPTY),
                "Left operand appended with null if it's present in the right, but not left one\n" +
                        "|   Entities with a property value in range 0 1 minvalue maxvalue\n" +
                        "|   Empty iterable"
        );
        checkIterable(new UnionIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Union\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new MinusIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Minus\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new IntersectionIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Intersection\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new ConcatenationIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Concatenation\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new PropertiesIterable(txn, 0, 1), "Entities with property sorted by its value 0 1");
        checkIterable(new PropertyValueIterable(txn, 0, 1, "value"), "Entities with specified property value 0 1 value");
        checkIterable(new EntitiesOfTypeIterable(txn, 0), "All entities of specific type 0");
        checkIterable(new EntitiesOfTypeRangeIterable(txn, 0, 3, 8), "Entities of specific type within id range 0 3 8");
        checkIterable(new EntitiesWithLinkIterable(txn, 0, 1), "Entities with link 0 1");
        checkIterable(new EntitiesWithLinkSortedIterable(txn, 0, 1, 2, 3), "Entities with link 0 1");
        checkIterable(new SingleEntityIterable(txn, new PersistentEntityId(0, 1)), "Single entity 0 1");
        //noinspection ComparatorMethodParameterNotUsed
        checkIterable(new MergeSortedIterableWithValueGetter(txn, new ArrayList<EntityIterable>(), new ComparableGetter() {
            @Override
            public Comparable select(Entity entity) {
                return null;
            }
        }, new Comparator<Comparable<Object>>() {
            @Override
            public int compare(Comparable<Object> o1, Comparable<Object> o2) {
                return 0;
            }
        }), "Merge sorted iterables 0");
        checkIterable(new EntitiesWithPropertyIterable(txn, 0, 1), "Entities with property 0 1");
        checkIterable(new EntitiesWithBlobIterable(txn, 0, 1), "Entities with blob 0 1");
        checkIterable(new TakeEntityIterable(txn, EntityIterableBase.EMPTY, 0),
                "Take iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new EntityReverseIterable(txn, EntityIterableBase.EMPTY),
                "Reversed iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SortIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY, 0, 2, false),
                "Sorting iterable 0 2 1\n" +
                        "|   Empty iterable"
        );
        checkIterable(new DistinctIterable(txn, EntityIterableBase.EMPTY),
                "Distinct iterable\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SelectManyDistinctIterable(txn, EntityIterableBase.EMPTY, 0),
                "SelectMany distinct iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SortResultIterable(txn, EntityIterableBase.EMPTY), "Empty iterable");
        checkIterable(new SelectDistinctIterable(txn, EntityIterableBase.EMPTY, 0),
                "Select distinct iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new SkipEntityIterable(txn, EntityIterableBase.EMPTY, 0),
                "Skip iterable 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new ExcludeNullIterableDecorator(txn, EntityIterableBase.EMPTY),
                "Exclude null\n" +
                        "|   Empty iterable"
        );
        IntHashMap<String> map = new IntHashMap<>();
        map.put(3, "value3");
        map.put(4, "value2");
        checkIterable(new EntityFromLinkSetIterable(txn, new PersistentEntityId(0, 1), map),
                "Outgoing links of a set from an entity 0 1  2 links: 3 4");
        checkIterable(new EntityFromLinksIterable(txn, new PersistentEntityId(0, 1), 2),
                "Outgoing links from an entity 0 1 2");
        checkIterable(new EntityToLinksIterable(txn, new PersistentEntityId(0, 1), 2, 3),
                "Incoming links to an entity 0 1 2 3");
        checkIterable(new FilterEntityTypeIterable(txn, 0, EntityIterableBase.EMPTY),
                "Filter source iterable by entity type 0\n" +
                        "|   Empty iterable"
        );
        checkIterable(new FilterLinksIterable(txn, 0, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                "Filter source iterable by links set 0\n" +
                        "|   Empty iterable\n" +
                        "|   Empty iterable"
        );
        // XD-441
        checkIterable(new IntersectionIterable(txn,
                        new IntersectionIterable(txn,
                                new IntersectionIterable(txn,
                                        new UnionIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY)),
                                new IntersectionIterable(txn,
                                        new UnionIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY))),
                        new IntersectionIterable(txn,
                                new IntersectionIterable(txn,
                                        new UnionIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY)),
                                new IntersectionIterable(txn,
                                        new UnionIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY),
                                        new MinusIterable(txn, EntityIterableBase.EMPTY, EntityIterableBase.EMPTY)))),
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
