/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.env.*;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;

public abstract class JMHEnvBenchmarkBase {

    private TemporaryFolder temporaryFolder;
    public Environment env;
    public Store store;

    public void setup() throws IOException {
        Log.invalidateSharedCache();
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final File testsDirectory = temporaryFolder.newFolder("data");
        env = Environments.newInstance(testsDirectory,
                adjustEnvironmentConfig(new EnvironmentConfig().setLogFileSize(32768)));
        store = env.computeInTransaction(txn -> env.openStore("JMHEnvBenchmark", getStoreConfig(), txn));
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        if (env != null) {
            env.close();
            env = null;
        }
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }

    protected abstract StoreConfig getStoreConfig();

    protected EnvironmentConfig adjustEnvironmentConfig(@NotNull final EnvironmentConfig ec) {
        return ec;
    }
}
