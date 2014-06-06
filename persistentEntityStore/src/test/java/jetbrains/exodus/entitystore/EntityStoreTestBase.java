/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.io.File;

public abstract class EntityStoreTestBase extends TestBase {
    private static final String TEMP_FOLDER = TestUtil.createTempDir().getAbsolutePath();

    private PersistentEntityStoreImpl store;
    @Nullable
    private PersistentStoreTransaction txn;
    protected boolean isPartiallyTornDown;
    protected boolean shouldCleanopOnTearDown;
    private String databaseFolder;

    protected boolean needsImplicitTxn() {
        return true;
    }

    @Override
    protected void setUp() throws Exception {
        isPartiallyTornDown = false;
        shouldCleanopOnTearDown = true;
        store = createStore(getDatabaseFolder());
        if (needsImplicitTxn()) {
            txn = store.beginTransaction();
        }
    }

    protected String getDatabaseFolder() {
        if (databaseFolder == null) {
            databaseFolder = initTempFolder();
        }
        return databaseFolder;
    }

    public static PersistentEntityStoreImpl createStore(String dbTempFolder) throws Exception {
        return PersistentEntityStores.newInstance(dbTempFolder);
    }

    @Override
    protected void tearDown() throws Exception {
        if (!isPartiallyTornDown) {
            if (txn != null) {
                txn.abort();
            }
            store.close();
        }
        if (shouldCleanopOnTearDown) {
            cleanUp(store.getLocation());
            databaseFolder = null;
        }
    }

    public static String initTempFolder() {
        // Configure temp folder for database
        final String location = randomTempFolder();
        final File tempFolder = new File(location);
        if (!tempFolder.mkdirs()) {
            Assert.fail("Can't create directory at " + location);
        }
        if (log.isInfoEnabled()) {
            log.info("Temporary data folder created: " + location);
        }
        return tempFolder.getAbsolutePath();
    }

    public static void cleanUp(String location) {
        final File tempFolder = new File(location);
        if (log.isInfoEnabled()) {
            log.info("Cleaning data folder: " + location);
        }
        IOUtil.deleteRecursively(tempFolder);
        IOUtil.deleteFile(tempFolder);
    }

    protected final PersistentEntityStoreImpl getEntityStore() {
        return store;
    }

    protected final PersistentStoreTransaction getStoreTransaction() {
        return txn;
    }

    @NotNull
    protected final PersistentStoreTransaction getStoreTransactionSafe() {
        final PersistentStoreTransaction result = txn;
        Assert.assertNotNull(result);
        return result;
    }

    @Override
    protected String getArtifactsPath() {
        return "." + File.separatorChar + "testartifacts" + File.separatorChar;
    }

    @Override
    protected File[] getFilesToBackup() {
        final File tempFolder = new File(store.getLocation());
        if (txn != null) {
            txn.abort();
        }
        store.close();
        isPartiallyTornDown = true;
        return new File[]{tempFolder};
    }

    public static String randomTempFolder() {
        return TEMP_FOLDER + Math.random();
    }

    protected void reinit() throws Exception {
        shouldCleanopOnTearDown = false;
        tearDown();
        setUp();
    }
}
