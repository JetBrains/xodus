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
package jetbrains.exodus.benchmark.tree;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.benchmark.BenchmarkTestBase;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

@SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
public abstract class TreeBenchmarkTestBase extends BenchmarkTestBase {

    protected Log log = null;
    protected ITree t;
    protected ITreeMutable tm;

    protected abstract ITreeMutable createMutableTree(final boolean hasDuplicates, final int structureId);

    protected abstract ITree openTree(long address, boolean hasDuplicates);

    protected Log createLog() {
        LogConfig config = createLogConfig();
        try {
            config.setDir(temporaryFolder.newFolder("logData"));
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }
        return new Log(config);
    }

    protected LogConfig createLogConfig() {
        Log.invalidateSharedCache();
        return new LogConfig().setNonBlockingCache(true);
    }

    @Before
    public void create() {
        log = createLog();
        //System.out.println("Log created: " + log);
        tm = createMutableTree(false, 1);
    }

    @After
    public void destroy() {
        //System.out.println("Log closing: " + log);
        log.close();
    }

}
