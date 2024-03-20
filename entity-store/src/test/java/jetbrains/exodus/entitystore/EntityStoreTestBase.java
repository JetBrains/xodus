/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class EntityStoreTestBase extends TestBase {

    private static final String TEMP_FOLDER = TestUtil.createTempDir().getAbsolutePath();

    private PersistentEntityStoreImpl store;
    @Nullable
    private PersistentStoreTransaction txn;
    protected boolean isPartiallyTornDown;
    protected boolean shouldCleanopOnTearDown;
    private String databaseFolder;

    protected boolean needsImplicitTxn() {
        for (final String testName : casesThatDontNeedExplicitTxn()) {
            if (getName().equals(testName)) {
                return false;
            }
        }
        return true;
    }

    protected String[] casesThatDontNeedExplicitTxn() {
        return new String[0];
    }

    @Override
    protected void setUp() throws Exception {
        Log.invalidateSharedCache();
        isPartiallyTornDown = false;
        shouldCleanopOnTearDown = true;
        openStore();
        if (needsImplicitTxn()) {
            txn = store.beginTransaction();
        }
    }

    protected PersistentEntityStoreImpl openStore() {
        return store = createStoreInternal(getDatabaseFolder());
    }

    protected String getDatabaseFolder() {
        if (databaseFolder == null) {
            databaseFolder = initTempFolder();
        }
        return databaseFolder;
    }

    protected PersistentEntityStoreImpl createStoreInternal(String dbTempFolder) {
        return createStore(dbTempFolder);
    }

    public static PersistentEntityStoreImpl createStore(String dbTempFolder) {
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

    public void reportInLogEntityIterableCacheStats() {
        EntityIterableCacheStatistics cacheStats = getEntityStore().getEntityIterableCache().getStats();
        long size = ((EntityIterableCacheAdapter) getEntityStore().getEntityIterableCache().getCacheAdapter()).size();
        long jobsEnqueued = cacheStats.getTotalJobsEnqueued();
        long jobsObsolete = cacheStats.getTotalJobsObsolete();
        long jobsOverdue = cacheStats.getTotalJobsOverdue();
        logger.info(
                "[EntityIterableCache] size = {}, hits={}, misses={}, hitRate={}, count={}",
                size, cacheStats.getTotalHits(), cacheStats.getTotalMisses(), cacheStats.getHitRate(), getEntityStore().getEntityIterableCache().count()
        );
        logger.info(
                "[EntityIterableCache Jobs] enqueued={}, obsolete={}, overdue={}",
                jobsEnqueued, jobsObsolete, jobsOverdue
        );
        logger.info(
                "[EntityIterableCache Count] hits={}, misses={}, hitRate={}",
                cacheStats.getTotalCountHits(), cacheStats.getTotalCountMisses(), cacheStats.getCountHitRate()
        );
    }

    public static String initTempFolder() {
        // Configure temp folder for database
        final String location = randomTempFolder();
        final File tempFolder = new File(location);
        if (!tempFolder.mkdirs()) {
            Assert.fail("Can't create directory at " + location);
        }
        if (logger.isInfoEnabled()) {
            logger.info("Temporary data folder created: " + location);
        }
        return tempFolder.getAbsolutePath();
    }

    public static void cleanUp(String location) {
        final File tempFolder = new File(location);
        if (logger.isInfoEnabled()) {
            logger.info("Cleaning data folder: " + location);
        }
        IOUtil.deleteRecursively(tempFolder);
        IOUtil.deleteFile(tempFolder);
    }

    public void transactional(@NotNull final PersistentStoreTransactionalExecutable executable) {
        store.executeInTransaction(wrap(executable));
    }

    public void transactionalExclusive(@NotNull final PersistentStoreTransactionalExecutable executable) {
        store.executeInExclusiveTransaction(wrap(executable));
    }

    public void transactionalReadonly(@NotNull final PersistentStoreTransactionalExecutable executable) {
        store.executeInReadonlyTransaction(wrap(executable));
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

    @NotNull
    private static StoreTransactionalExecutable wrap(@NotNull final EntityStoreTestBase.PersistentStoreTransactionalExecutable executable) {
        return (StoreTransactionalExecutable) txn -> executable.execute((PersistentStoreTransaction) txn);
    }

    public interface PersistentStoreTransactionalExecutable {

        void execute(@NotNull final PersistentStoreTransaction txn);
    }

    public static List<Entity> toList(Iterable<Entity> it) {
        final List<Entity> result = new ArrayList<>();
        for (Entity entity : it) {
            result.add(entity);
        }
        return result;
    }
}
