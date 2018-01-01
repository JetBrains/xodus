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

import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.SingleEntityIterable;
import jetbrains.exodus.entitystore.iterate.SortIndirectIterable;
import org.junit.Assert;

@SuppressWarnings({"ConstantConditions"})
public class SortIndirectIterableTest extends EntityStoreTestBase {

    private static final String ENTITY_TYPE = "Issue";
    private static final String LINK_NAME = "fixedInBuild";

    public void testIterateWithNullLinks() {
        final PersistentStoreTransaction txn = getStoreTransaction();

        // Create single Issue entity with no links.
        final Entity issue = txn.newEntity(ENTITY_TYPE);
        txn.flush();

        // Iterate over all issues sorted by fixedInBuild link when sorted list of links is empty.
        checkSortedCount(txn, EntityIterableBase.EMPTY, 1);

        // The same, but the sorted list of links contains single null.
        SingleEntityIterable singleNullIterable = new SingleEntityIterable(txn, null);
        checkSortedCount(txn, singleNullIterable, 1);

        // Set fixedInBuild link to our issue
        issue.setLink(LINK_NAME, txn.newEntity("Build"));
        txn.flush();

        // Sort one issue with link by the link.
        SingleEntityIterable singleNotNullIterable =
                new SingleEntityIterable(txn, issue.getLink(LINK_NAME).getId());
        checkSortedCount(txn, singleNotNullIterable, 1);

        // Create another Issue entity with no links.
        txn.newEntity(ENTITY_TYPE);
        txn.flush();

        // Sort issues. Sorted links consists of single not null element.
        checkSortedCount(txn, singleNotNullIterable, 2);

        // Sort issues. Sorted links consists of null and not null elements.
        checkSortedCount(txn,
                (EntityIterableBase) singleNotNullIterable.union(singleNullIterable), 2);

        // Delete fixedInBuild link from the first issue
        issue.deleteLinks(LINK_NAME);
        txn.flush();

        // Sort issues with empty sortedLinks.
        checkSortedCount(txn, EntityIterableBase.EMPTY, 2);

        // Sort issues with sortedLinks containing single null.
        checkSortedCount(txn, singleNullIterable, 2);
    }

    @SuppressWarnings({"UnusedDeclaration"})
    private void checkSortedCount(PersistentStoreTransaction txn, EntityIterableBase sortedLinks, int count) {
        int cnt = 0;
        for (Entity e : new SortIndirectIterable(txn, getEntityStore(), ENTITY_TYPE, sortedLinks, LINK_NAME,
                (EntityIterableBase) txn.getAll(ENTITY_TYPE), null, null)) {
            cnt++;
        }
        Assert.assertEquals(count, cnt);
    }
}
