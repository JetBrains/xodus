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

import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.entitystore.iterate.EntityIteratorWithPropId;

import java.util.Set;

@SuppressWarnings({"OverlyLongMethod", "ConstantConditions"})
public class EntityFromLinkSetTests extends EntityStoreTestBase {

    public void testSimple() {
        final StoreTransaction txn = getStoreTransaction();
        final Entity i1 = txn.newEntity("Issue");
        final Entity i2 = txn.newEntity("Issue");
        final Entity i3 = txn.newEntity("Issue");
        final Entity i4 = txn.newEntity("Issue");

        i1.addLink("dup", i2);
        i1.addLink("hup", i3);
        i1.addLink("hup", i4);
        i2.addLink("dup", i3);

        txn.flush();

        final Set<String> names = new HashSet<>(2);
        names.add("dup");
        names.add("hup");

        EntityIteratorWithPropId it;

        for (int i = 0; i < 2; i++) {
            it = (EntityIteratorWithPropId) i1.getLinks(names).iterator();

            assertTrue(it.hasNext());
            assertEquals(i2, it.next());
            assertEquals("dup", it.currentLinkName());
            assertTrue(it.hasNext());
            assertEquals(i3, it.next());
            assertEquals("hup", it.currentLinkName());
            assertTrue(it.hasNext());
            assertEquals(i4, it.next());
            assertEquals("hup", it.currentLinkName());
            assertFalse(it.hasNext());

            getEntityStore().getAsyncProcessor().waitForJobs(100);
        }

        for (int i = 0; i < 2; i++) {
            it = (EntityIteratorWithPropId) i2.getLinks(names).iterator();

            assertTrue(it.hasNext());
            assertEquals(i3, it.next());
            assertEquals("dup", it.currentLinkName());
            assertFalse(it.hasNext());

            getEntityStore().getAsyncProcessor().waitForJobs(100);
        }
    }
}
