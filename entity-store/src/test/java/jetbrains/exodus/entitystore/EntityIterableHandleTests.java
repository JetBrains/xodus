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
import jetbrains.exodus.entitystore.iterate.ConstantEntityIterableHandle;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.EntityIterableHandleBase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.security.SecureRandom;
import java.util.Set;

public class EntityIterableHandleTests extends EntityStoreTestBase {

    public void testTrivial() {
        final EntityIterableHandleBase h = new ConstantEntityIterableHandle(getEntityStore(), EntityIterableType.EMPTY) {
            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                for (int i = 0; i < 31; ++i) {
                    builder.append('0');
                }
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                for (int i = 0; i < 31; ++i) {
                    hash.apply("0");
                }
            }
        };
        Assert.assertEquals("00000000000000000000000000000000", h.toString());
    }

    public void testDistribution() {
        final SecureRandom rnd = new SecureRandom();
        final Set<EntityIterableHandleBase.EntityIterableHandleHash> set = new HashSet<>();
        for (int i = 0; i < 1000000; ++i) {
            final EntityIterableHandleBase.EntityIterableHandleHash h = new EntityIterableHandleBase.EntityIterableHandleHash(getEntityStore());
            h.apply("00000000000000000000000000000000");
            final int intsCount = rnd.nextInt(40) + 10;
            for (int j = 0; j < intsCount; ++j) {
                h.applyDelimiter();
                h.apply(rnd.nextInt() & 0xff);
            }
            h.computeHashCode();
            // in case of poor distribution, birthday paradox will give assertion quite soon
            if (!set.add(h)) {
                Assert.assertTrue(false);
            }
        }
    }

    public void test_XD_438() {
        final EntityIterableHandleBase h = new ConstantEntityIterableHandle(getEntityStore(), EntityIterableType.REVERSE) {
            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(EntityIterableType.SINGLE_ENTITY.getType());
                builder.append('-');
                new PersistentEntityId(1000000000, 10000000000000000L).toString(builder);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(EntityIterableType.SINGLE_ENTITY.getType());
                hash.applyDelimiter();
                new PersistentEntityId(1000000000, 10000000000000000L).toHash(hash);
            }
        };
        Assert.assertEquals("Reversed iterable\n" +
                "|   Single entity 1000000000 10000000000000000", EntityIterableBase.getHumanReadablePresentation(h));
    }
}
