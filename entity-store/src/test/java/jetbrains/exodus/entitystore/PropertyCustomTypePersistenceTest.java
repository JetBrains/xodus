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

import jetbrains.exodus.TestFor;
import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.bindings.ComparableBinding;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;

public class PropertyCustomTypePersistenceTest extends EntityStoreTestBase {

    @Override
    protected String[] casesThatDontNeedExplicitTxn() {
        return new String[]{"testPersistentCustomPropertyType"};
    }

    @TestFor(issues = "XD-555")
    public void testPersistentCustomPropertyType() throws Exception {
        PersistentEntityStoreImpl store = getEntityStore();

        registerDatatype(store);

        store.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                Entity testEntity = txn.newEntity("Entity");
                testEntity.setProperty("property", new MockData(42));
                txn.saveEntity(testEntity);
            }
        });

        store.close();
        store = openStore();

        registerDatatype(store);

        store.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                final Entity entity = txn.getAll("Entity").getFirst();
                assertNotNull(entity);
                assertEquals(new MockData(42), entity.getProperty("property"));
            }
        });
    }

    private void registerDatatype(PersistentEntityStore store) {
        StoreTransaction txn = store.beginTransaction();
        store.registerCustomPropertyType(txn, MockData.class, new MockBinding());
        if (!txn.commit()) {
            throw new IllegalStateException("Couldn't register ByteBufferWrapper property type.");
        }
    }

    private static class MockData implements Comparable<MockData> {
        private final int value;

        private MockData(int value) {
            this.value = value;
        }

        @Override
        public int compareTo(@NotNull MockData o) {
            return value - o.value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockData mockData = (MockData) o;

            return value == mockData.value;

        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    private static class MockBinding extends ComparableBinding {
        @Override
        public MockData readObject(@NotNull final ByteArrayInputStream stream) {
            return new MockData(BindingUtils.readInt(stream));
        }

        @Override
        public void writeObject(@NotNull final LightOutputStream output, @NotNull final Comparable object) {
            output.writeUnsignedInt(((MockData) object).value ^ 0x80000000);
        }
    }
}