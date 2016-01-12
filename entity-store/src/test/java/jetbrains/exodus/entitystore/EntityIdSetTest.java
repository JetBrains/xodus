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

import jetbrains.exodus.entitystore.util.EntityIdSet;
import org.junit.Assert;

public class EntityIdSetTest extends EntityStoreTestBase {

    public void testEntityIdSet() {
        final EntityIdSet set = new EntityIdSet();
        for (int i = 0; i < 10; ++i) {
            for (long j = 0; j < 1000; ++j) {
                set.add(i, j);
            }
            for (long j = 0; j < 1000; ++j) {
                Assert.assertTrue(set.contains(i, j));
            }
            for (long j = 0; j < 1000; ++j) {
                Assert.assertFalse(set.contains(i + 1, j));
            }
        }
        Assert.assertFalse(set.contains(null));
        set.add(null);
        Assert.assertTrue(set.contains(null));
    }

    public void testEntityIdSetIterator() {
        final EntityIdSet set = new EntityIdSet();
        for (int i = 0; i < 10; ++i) {
            for (long j = 0; j < 1000; ++j) {
                set.add(i, j);
            }
        }
        EntityIdSet sample = new EntityIdSet();
        for (final EntityId entityId : set) {
            Assert.assertTrue(set.contains(entityId));
            Assert.assertFalse(sample.contains(entityId));
            sample.add(entityId);
        }
        Assert.assertFalse(sample.contains(null));
        set.add(null);
        sample = new EntityIdSet();
        for (final EntityId entityId : set) {
            Assert.assertTrue(set.contains(entityId));
            Assert.assertFalse(sample.contains(entityId));
            sample.add(entityId);
        }
        Assert.assertTrue(sample.contains(null));
    }
}
