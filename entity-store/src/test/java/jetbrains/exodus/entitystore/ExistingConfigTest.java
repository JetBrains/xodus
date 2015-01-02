/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.env.*;
import jetbrains.exodus.util.IOUtil;
import junit.framework.TestCase;
import org.junit.Assert;

import java.io.File;

public class ExistingConfigTest extends TestCase {

    private Environment env;
    private File dbPath;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        dbPath = new File(EntityStoreTestBase.randomTempFolder());
        //noinspection ResultOfMethodCallIgnored
        dbPath.mkdir();
        final String path = dbPath.getPath();
        env = Environments.newInstance(path, new EnvironmentConfig());
    }

    @Override
    protected void tearDown() throws Exception {
        env.close();
        IOUtil.deleteRecursively(dbPath);
        super.tearDown();
    }

    public void testConfig() {
        final StoreConfig expectedConfig = StoreConfig.WITHOUT_DUPLICATES; // prefixing wtf
        final String name = "testDatabase";

        Transaction txn = env.beginTransaction();
        env.openStore(name, expectedConfig, txn);
        txn.commit();

        txn = env.beginTransaction();
        final Store store = env.openStore(name, StoreConfig.USE_EXISTING_READONLY, txn);
        Assert.assertEquals(expectedConfig, store.getConfig());
        txn.commit();
    }
}
