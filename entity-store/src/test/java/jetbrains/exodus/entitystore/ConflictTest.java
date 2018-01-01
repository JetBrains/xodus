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

public class ConflictTest extends EntityStoreTestBase {

    public void testCreateTable() throws Exception {
        final StoreTransaction txn = getStoreTransaction();
        txn.newEntity("Issue").setProperty("name", "i1");
        PersistentStoreTransaction txn2 = getEntityStore().beginTransaction();
        txn2.newEntity("Issue").setProperty("name", "i0");
        assertEquals(1, txn2.getAll("Issue").size());
        txn2.commit();
        assertEquals(1, txn.getAll("Issue").size());
        assertEquals(txn, getEntityStore().getCurrentTransaction());
        assertFalse(txn.flush());
        //replay txn
        txn.newEntity("Issue").setProperty("name", "i1"); // this way issue gets a new id, so iterates after i0
        //end replay
        txn.flush();
        assertEquals(2, txn.getAll("Issue").size());
        reinit();
        assertEquals(2, getStoreTransaction().getAll("Issue").size());
        int i = 0;
        for (Entity issue : getStoreTransaction().getAll("Issue")) {
            assertEquals(issue.getProperty("name"), "i" + i++);
        }
        assertEquals(2, i);
    }

    public void testSequences() throws Exception {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final Sequence seq = txn.getSequence("Seq");
        seq.set(0);
        txn.flush();
        seq.increment();
        txn.apply(); // emulate race condition on commit using test-exposed method
        PersistentStoreTransaction txn2 = getEntityStore().beginTransaction();
        txn2.newEntity("Whatever");
        txn2.commit(); // this commit is executed after seq.increment(), should persist new value to db
        assertFalse(txn.doFlush());
        txn.flush();
        reinit();
        assertEquals(1, getStoreTransaction().getSequence("Seq").get());
    }
}
