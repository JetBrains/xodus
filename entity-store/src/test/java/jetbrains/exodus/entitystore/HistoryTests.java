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

import org.junit.Assert;

import java.util.List;

@SuppressWarnings("ConstantConditions")
public class HistoryTests extends EntityStoreTestBase {

    public void testNonHistoricalItem() {
        final PersistentStoreTransaction txn = getStoreTransaction();

        final PersistentEntity entity = txn.newEntity("Issue");
        entity.setProperty("name", "name");
        entity.setProperty("fame", "lame");
        txn.flush();

        final int version = entity.getVersion();
        final PersistentEntityId persistentId = new PersistentEntityId(entity.getId(), version);
        entity.setProperty("name", "lame");
        entity.setProperty("fame", "game");
        txn.flush();

        final PersistentStoreTransaction snapshot = txn.getSnapshot();
        entity.deleteProperty("name");
        try {
            entity.newVersion(snapshot);
        } finally {
            snapshot.abort();
        }
        txn.flush();

        final Entity historyItem = txn.getEntity(persistentId);
        assert historyItem != null;
        Assert.assertNotNull(historyItem);
        Assert.assertEquals("lame", historyItem.getProperty("name"));
        Assert.assertEquals("game", historyItem.getProperty("fame"));
    }

    public void testSingleHistoryItem() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity entity = txn.newEntity("Issue");
        entity.setProperty("name", "name");
        txn.flush();
        final PersistentStoreTransaction snapshot = txn.getSnapshot();
        try {
            entity.newVersion(snapshot);
        } finally {
            snapshot.abort();
        }
        entity.deleteProperty("name");
        txn.flush();
        Assert.assertNull(entity.getProperty("name"));
        final Entity historyItem = entity.getPreviousVersion();
        assert historyItem != null;
        Assert.assertNotNull(historyItem);
        Assert.assertEquals("name", historyItem.getProperty("name"));
    }

    public void testForwardEnumeration() {
        final PersistentEntityId id = make10Versions();
        final StoreTransaction txn = getStoreTransaction();
        Entity entity = new PersistentEntity(
                (PersistentEntityStoreImpl) txn.getStore(), new PersistentEntityId(id, 0));
        for (int i = 0; ; ++i) {
            Assert.assertEquals(i, entity.getVersion());
            Assert.assertEquals(i, ((Integer) entity.getProperty("version")).intValue());
            entity = entity.getNextVersion();
            if (entity == null) {
                Assert.assertEquals(9, i);
                break;
            }
        }
    }

    public void testBackwardEnumeration() {
        final EntityId id = make10Versions();
        final StoreTransaction txn = getStoreTransaction();
        Entity entity = txn.getEntity(id);
        assert entity != null;
        for (int i = 0; ; ++i) {
            Assert.assertEquals(9 - i, entity.getVersion());
            Assert.assertEquals(9 - i, ((Integer) entity.getProperty("version")).intValue());
            entity = entity.getPreviousVersion();
            if (entity == null) {
                Assert.assertEquals(0, 9 - i);
                break;
            }
        }
    }

    public void testHistoryList() {
        final EntityId id = make10Versions();
        final StoreTransaction txn = getStoreTransaction();
        Entity entity = txn.getEntity(id);
        assert entity != null;
        final List<Entity> history = entity.getHistory();
        Assert.assertEquals(9, history.size());
        int i = 0;
        for (final Entity issue : history) {
            Assert.assertEquals(8 - i++, ((Integer) issue.getProperty("version")).intValue());
        }
    }

    public void testClearHistory() {
        final EntityId id = make10Versions();
        final PersistentStoreTransaction txn = getStoreTransaction();
        PersistentEntity entity = txn.getEntity(id);
        Assert.assertNotNull(entity);
        txn.clearHistory("Issue");
        Assert.assertEquals(0, entity.getHistory().size());
        entity = txn.getEntity(id);
        Assert.assertNotNull(entity);
        txn.flush();
        final PersistentStoreTransaction snapshot = txn.getSnapshot();
        try {
            entity.newVersion(snapshot);
        } finally {
            snapshot.abort();
        }
        entity.setProperty("version", 1);
        txn.flush();
        final List<Entity> history = entity.getHistory();
        Assert.assertEquals(1, history.size());
        for (final Entity issue : history) {
            Assert.assertEquals(9, ((Integer) issue.getProperty("version")).intValue());
        }
    }

    public void testBlobHistory() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity entity = txn.newEntity("Issue");
        entity.setBlobString("name", "name");
        txn.flush();
        final PersistentStoreTransaction snapshot = txn.getSnapshot();
        try {
            entity.newVersion(snapshot);
        } finally {
            snapshot.abort();
        }
        entity.setBlobString("name", "name1");
        txn.flush();
        Assert.assertEquals("name1", entity.getBlobString("name"));
        final Entity historyItem = entity.getPreviousVersion();
        assert historyItem != null;
        Assert.assertNotNull(historyItem);
        Assert.assertEquals("name", historyItem.getBlobString("name"));
    }

    public void testBlobHistory2() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity entity = txn.newEntity("Issue");
        entity.setBlobString("name", "name");
        txn.flush();
        final PersistentStoreTransaction snapshot = txn.getSnapshot();
        try {
            entity.newVersion(snapshot);
        } finally {
            snapshot.abort();
        }
        entity.deleteBlob("name");
        txn.flush();
        final Entity historyItem = entity.getPreviousVersion();
        assert historyItem != null;
        Assert.assertNotNull(historyItem);
        Assert.assertEquals("name", historyItem.getBlobString("name"));
    }

    private PersistentEntityId make10Versions() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity entity = txn.newEntity("Issue");
        entity.setProperty("name", "name");
        entity.setProperty("version", 0);
        txn.flush();
        for (int i = 1; i < 10; ++i) {
            final PersistentStoreTransaction snapshot = txn.getSnapshot();
            try {
                entity.newVersion(snapshot);
            } finally {
                snapshot.abort();
            }
            entity.setProperty("version", i);
            txn.flush();
        }
        return (PersistentEntityId) entity.getId();
    }

}
