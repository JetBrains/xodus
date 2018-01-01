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
import org.junit.Assert;

public class ReplayTest extends EntityStoreTestBase {

    public static final int CACHE_SIZE = 16384;
    public static final int CHANGES_SIZE = 500;

    private int defaultCacheSize;

    @Override
    protected void setUp() throws Exception {
        defaultCacheSize = PersistentEntityStoreConfig.DEFAULT.getEntityIterableCacheSize();
        PersistentEntityStoreConfig.DEFAULT.setEntityIterableCacheSize(CACHE_SIZE);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        PersistentEntityStoreConfig.DEFAULT.setEntityIterableCacheSize(defaultCacheSize);
        super.tearDown();
    }

    public void testPerformance() throws Exception {
        final PersistentStoreTransaction txn = getStoreTransaction();
        assertNotNull(txn);
        for (int i = 0; i < CACHE_SIZE; ++i) {
            final Entity entity = txn.newEntity("Issue");
            // entity.setProperty("description", "Test issue #" + i);
            entity.setProperty("size", i);
        }
        txn.flush();
        for (int i = 0; i < CACHE_SIZE; ++i) {
            final EntityIterableBase issues = findSome(txn, i);
            issues.getOrCreateCachedInstance(txn); // fill cache
        }
        final Entity[] change = new Entity[CHANGES_SIZE];
        for (int i = 0; i < CHANGES_SIZE; i++) {
            Entity issue = findSome(txn, 239 + i).getFirst();
            assertNotNull(issue);
            change[i] = issue;
        }
        System.out.println(System.currentTimeMillis());
        changeStuff(change);
        System.out.println(System.currentTimeMillis());
        System.out.println("----------");
        System.out.println(System.currentTimeMillis());
        replay(txn, change);
        System.out.println(System.currentTimeMillis());
    }

    public void testUpdate() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        assertNotNull(txn);
        final Entity entity = txn.newEntity("Issue");
        txn.flush();
        entity.setProperty("prop", "wat");
        Assert.assertEquals(1, txn.findWithPropSortedByValue("Issue", "prop").size());
        final PersistentStoreTransaction txn2 = getEntityStore().beginTransaction();
        Assert.assertEquals(0, txn2.findWithPropSortedByValue("Issue", "prop").size());
        txn2.newEntity("Issue");
        txn2.commit(); // conflict
        Assert.assertFalse(txn.flush());
        entity.setProperty("prop", "wat");
        Assert.assertEquals(1, txn.findWithPropSortedByValue("Issue", "prop").size());
    }

    private static void replay(PersistentStoreTransaction txn, Entity[] change) {
        txn.revert();
        changeStuff(change);
    }

    private static void changeStuff(final Entity[] change) {
        for (int i = 0; i < CHANGES_SIZE; i++) {
            change[i].setProperty("size", 666 + i);
        }
    }

    private static EntityIterableBase findSome(final PersistentStoreTransaction txn, final int index) {
        return (EntityIterableBase) txn.find("Issue", "size", index).
                union(txn.find("Issue", "size", CACHE_SIZE - index)).union(txn.find("Issue", "size", 12345678));
    }
}
