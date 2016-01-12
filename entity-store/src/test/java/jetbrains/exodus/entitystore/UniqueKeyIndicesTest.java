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

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.NanoSet;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.entitystore.metadata.Index;
import jetbrains.exodus.entitystore.metadata.IndexField;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class UniqueKeyIndicesTest extends EntityStoreTestBase {

    private Entity entity1;

    @Override
    protected boolean needsImplicitTxn() {
        return false;
    }

    public void test2ValidColumns() throws Exception {
        createData();
        testValidColumns(getEntityStore(), "column1", "column2");
        // test proper updating
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                entity1.setProperty("column0", 1);
                entity1.setProperty("column1", "o");
            }
        });
        testValidColumns(getEntityStore(), "column0"); // index for (column1, column2) should become obsolete
        testInvalidColumns(getEntityStore(), "column1", "column2"); // new index creates and checks constraint
    }

    public void test3ValidColumns() throws Exception {
        createData();
        testValidColumns(getEntityStore(), "column2", "column1", "column3");
    }

    public void test2InvalidColumns() throws Exception {
        createData();
        testInvalidColumns(getEntityStore(), "column0", "column2");
    }

    public void testNonExistingColumns() throws Exception {
        createData();
        testInvalidColumns(getEntityStore(), "column0", "column2", "column");
    }

    public void testRepeatingColumns() throws Exception {
        createData();
        testInvalidColumns(getEntityStore(), "column0", "column2", "column0");
    }

    public void testUpdatingIndices() throws Exception {
        createData();
        testValidColumns(getEntityStore(), "column2", "column1", "column3");
        testValidColumns(getEntityStore(), "column1", "column2", "column3");
    }

    public void testRecreateIndices_XD_393() {
        createData();
        testValidColumns(getEntityStore(), "column0", "column1"); // create index
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                entity1.setProperty("column0", 1);
                entity1.setProperty("column1", "o");
                txn.deleteUniqueKey(new TestIndex("column0", "column1"), Arrays.asList((Comparable) Integer.valueOf(0), "oo"));
                txn.insertUniqueKey(new TestIndex("column0", "column1"), Arrays.asList((Comparable) Integer.valueOf(1), "o"), entity1);
            }
        });
        testValidColumns(getEntityStore()); // remove index
        testValidColumns(getEntityStore(), "column0", "column1"); // recreate index
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
                    @Override
                    public void execute(@NotNull StoreTransaction txn) {
                        txn.insertUniqueKey(new TestIndex("column0", "column1"), Arrays.asList((Comparable) Integer.valueOf(1), "o"), entity1);
                    }
                });
            }
        }, InsertConstraintException.class);
    }

    private void createData() {
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                final Entity entity0 = txn.newEntity("Issue");
                entity0.setProperty("column0", 0);
                entity0.setProperty("column1", "o");
                entity0.setProperty("column2", 0L);
                entity0.setProperty("column3", 0.0);
                entity1 = txn.newEntity("Issue");
                entity1.setProperty("column0", 0);
                entity1.setProperty("column1", "oo");
                entity1.setProperty("column2", 0L);
                entity1.setProperty("column3", 0.0);
            }
        });
    }

    private static void testValidColumns(final PersistentEntityStore store, String... columns) {
        Throwable t = null;
        try {
            store.updateUniqueKeyIndices(columns.length == 0 ? new HashSet<Index>() : getIndices(columns));
        } catch (Throwable e) {
            t = e;
        }
        Assert.assertNull(t);
    }

    private static void testInvalidColumns(final PersistentEntityStore store, String... columns) {
        Throwable t = null;
        try {
            store.updateUniqueKeyIndices(getIndices(columns));
        } catch (Throwable e) {
            t = e;
            if (logger.isInfoEnabled()) {
                logger.info("Expected throwable found", t);
            }
        }
        Assert.assertNotNull(t);
    }

    private static Set<Index> getIndices(String... columns) {
        return new NanoSet<Index>(new TestIndex(columns));
    }

    private static final class TestIndex implements Index {

        private final String[] columns;


        @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
        private TestIndex(String... columns) {
            this.columns = columns;
        }

        @Override
        public List<IndexField> getFields() {
            final List<IndexField> result = new ArrayList<>();
            for (final String column : columns) {
                result.add(new TestField(column));
            }
            return result;
        }

        @Override
        public Set<String> getEntityTypesToIndex() {
            return new NanoSet<>("Issue");
        }

        @Override
        public String getOwnerEntityType() {
            return "Issue";
        }
    }

    private static final class TestField implements IndexField {

        private final String name;

        private TestField(String name) {
            this.name = name;
        }

        @Override
        public boolean isProperty() {
            return true;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
