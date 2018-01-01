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

import jetbrains.exodus.entitystore.iterate.EntityIdSet;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import jetbrains.exodus.entitystore.util.ImmutableSingleTypeEntityIdBitSet;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Iterator;

public class EntityIdSetTest extends EntityStoreTestBase {

    public void testEntityIdSet() {
        EntityIdSet set = EntityIdSetFactory.newSet();
        for (int i = 0; i < 10; ++i) {
            for (long j = 0; j < 1000; ++j) {
                set = set.add(i, j);
            }
            for (long j = 0; j < 1000; ++j) {
                Assert.assertTrue(set.contains(i, j));
            }
            for (long j = 0; j < 1000; ++j) {
                Assert.assertFalse(set.contains(i + 1, j));
            }
        }
        Assert.assertEquals(-1, set.count());
        Assert.assertFalse(set.contains(null));
        set = set.add(null);
        Assert.assertTrue(set.contains(null));
    }

    public void testEntityIdSetIterator() {
        EntityIdSet set = EntityIdSetFactory.newSet();
        for (int i = 0; i < 10; ++i) {
            for (long j = 0; j < 1000; ++j) {
                set = set.add(i, j);
            }
        }
        EntityIdSet sample = EntityIdSetFactory.newSet();
        for (final EntityId entityId : set) {
            Assert.assertTrue(set.contains(entityId));
            Assert.assertFalse(sample.contains(entityId));
            sample = sample.add(entityId);
        }
        Assert.assertFalse(sample.contains(null));
        set = set.add(null);
        sample = EntityIdSetFactory.newSet();
        for (final EntityId entityId : set) {
            Assert.assertTrue(set.contains(entityId));
            Assert.assertFalse(sample.contains(entityId));
            sample = sample.add(entityId);
        }
        Assert.assertTrue(sample.contains(null));
    }

    public void testEntityIdSetCount() {
        EntityIdSet set = EntityIdSetFactory.newSet();
        for (int i = 0; i < 100; ++i) {
            set = set.add(7, i);
        }
        Assert.assertEquals(100, set.count());
    }

    public void testBitSet() {
        checkSet(0, 2, 7, 11, 55, 78);
        checkSet(4, 6, 7, 11, 55, 260);
        checkSet(4147483647L, 4147483648L, 4147483649L);
    }

    private void checkSet(long... data) {
        Arrays.sort(data);
        final int typeId = 7;
        ImmutableSingleTypeEntityIdBitSet set = new ImmutableSingleTypeEntityIdBitSet(typeId, data);
        assertEquals(data.length, set.count());
        long prevValue = data[0] - 20;
        for (int i = 0; i < data.length; i++) {
            long value = data[i];
            for (long k = prevValue + 1; k < value; k++) {
                assertFalse(set.contains(typeId, k));
                assertEquals(-1, set.indexOf(new PersistentEntityId(typeId, k)));
            }
            prevValue = value;
            assertTrue(set.contains(typeId, value));
            assertFalse(set.contains(3, value));
            assertEquals(i, set.indexOf(new PersistentEntityId(typeId, value)));
        }
        final long[] actualData = new long[data.length];
        int i = 0;
        for (final EntityId id : set) {
            assertEquals(typeId, id.getTypeId());
            actualData[i++] = id.getLocalId();
        }
        Assert.assertArrayEquals(data, actualData);
        Iterator<EntityId> reverseIterator = set.reverseIterator();
        while (reverseIterator.hasNext()) {
            EntityId next = reverseIterator.next();
            Assert.assertEquals(data[--i], next.getLocalId());
        }
        Assert.assertEquals(data[0], set.getFirst().getLocalId());
        Assert.assertEquals(data[data.length - 1], set.getLast().getLocalId());
    }
}
