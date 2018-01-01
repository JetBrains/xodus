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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.util.ByteIterableUtil;
import org.junit.Assert;

public class TestOrdering extends EntityStoreTestBase {

    public void testDirectOrder() {
        final StoreTransaction txn = getStoreTransaction();
        for (int i = 0; i < 200; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("whatever", true);
        }
        txn.flush();
        int i = 0;
        for (Entity e : txn.getAll("Issue")) {
            final EntityId id = e.getId();
            assertEquals(0, id.getTypeId());
            assertEquals(i++, id.getLocalId());
        }
    }

    public void testBindings() {
        final ArrayByteIterable entry0 = PropertyKey.propertyKeyToEntry(new PropertyKey(15, 71));
        final ArrayByteIterable entry1 = PropertyKey.propertyKeyToEntry(new PropertyKey(16, 24));
        final ArrayByteIterable entry2 = PropertyKey.propertyKeyToEntry(new PropertyKey(128, 24));
        final ArrayByteIterable entry3 = PropertyKey.propertyKeyToEntry(new PropertyKey(245, 71));

        Assert.assertTrue(ByteIterableUtil.compare(entry0, entry1) < 0);
        Assert.assertTrue(ByteIterableUtil.compare(entry0, entry2) < 0);
        Assert.assertTrue(ByteIterableUtil.compare(entry0, entry3) < 0);

        Assert.assertTrue(ByteIterableUtil.compare(entry1, entry2) < 0);
        Assert.assertTrue(ByteIterableUtil.compare(entry1, entry3) < 0);
        Assert.assertTrue(ByteIterableUtil.compare(entry1, entry0) > 0);

        Assert.assertTrue(ByteIterableUtil.compare(entry2, entry3) < 0);
        Assert.assertTrue(ByteIterableUtil.compare(entry2, entry1) > 0);
        Assert.assertTrue(ByteIterableUtil.compare(entry2, entry0) > 0);
    }

}
