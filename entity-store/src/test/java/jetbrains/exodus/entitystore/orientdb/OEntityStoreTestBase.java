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
package jetbrains.exodus.entitystore.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public abstract class OEntityStoreTestBase extends TestBase {

    private static final String TEMP_FOLDER = TestUtil.createTempDir().getAbsolutePath();

    private OrientDB orientDB;
    private PersistentEntityStore store;
    protected OSchemaBuddy schemaBuddy;

    protected boolean isPartiallyTornDown;
    protected boolean shouldCleanopOnTearDown;
    private String databaseFolder;


    @Override
    protected void setUp() throws Exception {
        isPartiallyTornDown = false;
        shouldCleanopOnTearDown = true;

        orientDB = OrientDB.embedded(getClass().getSimpleName(), OrientDBConfig.defaultConfig());
        if (orientDB.exists(getName())) {
            orientDB.drop(getName());
        }

        orientDB.create(getName(), ODatabaseType.MEMORY, "admin", "admin", "admin");

        openStore();
    }

    protected PersistentEntityStore openStore() {
        return store = createStoreInternal(getDatabaseFolder());
    }

    protected String getDatabaseFolder() {
        if (databaseFolder == null) {
            databaseFolder = initTempFolder();
        }
        return databaseFolder;
    }

    protected PersistentEntityStore createStoreInternal(String dbTempFolder) {
        return createStore(dbTempFolder);
    }

    public PersistentEntityStore createStore(String dbTempFolder) {
        ODatabaseProvider databaseProvider = new ODatabaseProvider() {
            @NotNull
            @Override
            public String getDatabaseLocation() {
                return "";
            }

            @NotNull
            @Override
            public OrientDB getDatabase() {
                return orientDB;
            }

            @NotNull
            @Override
            public ODatabaseSession acquireSession() {
                return orientDB.cachedPool(getName(), "admin", "admin").acquire();
            }

            @Override
            public void close() {
                orientDB.close();
            }
        };
        schemaBuddy = new OSchemaBuddyImpl(databaseProvider, true);
        return new OPersistentEntityStore(databaseProvider, getName(), Executors.newSingleThreadExecutor(), schemaBuddy);
    }

    @Override
    protected void tearDown() {
        if (!isPartiallyTornDown) {
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

    public void transactional(
            @NotNull final StoreTransactionalExecutable executable) {
        store.executeInTransaction(executable);
    }

    public void transactionalExclusive(
            @NotNull final StoreTransactionalExecutable executable) {
        store.executeInExclusiveTransaction(executable);
    }

    public void transactionalReadonly(
            @NotNull final StoreTransactionalExecutable executable) {
        store.executeInReadonlyTransaction(executable);
    }

    protected final PersistentEntityStore getEntityStore() {
        return store;
    }

    @Override
    protected String getArtifactsPath() {
        return "." + File.separatorChar + "testartifacts" + File.separatorChar;
    }

    @Override
    protected File[] getFilesToBackup() {
        final File tempFolder = new File(store.getLocation());
        store.close();
        isPartiallyTornDown = true;
        return new File[]{tempFolder};
    }

    public ODatabaseSession acquireSession() {
        return orientDB.cachedPool(getName(), "admin", "admin").acquire();
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
    private static StoreTransactionalExecutable wrap(
            @NotNull final EntityStoreTestBase.PersistentStoreTransactionalExecutable executable) {
        return txn -> executable.execute(
                (PersistentStoreTransaction) txn);
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
