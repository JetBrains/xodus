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
package jetbrains.exodus.vfs;

import jetbrains.exodus.env.EnvironmentTestsBase;
import jetbrains.exodus.log.LogConfig;
import org.junit.After;
import org.junit.Before;

public class VfsTestsBase extends EnvironmentTestsBase {

    protected VirtualFileSystem vfs;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        vfs = createVirtualFileSystem();
    }

    @Override
    protected void createEnvironment() {
        env = newContextualEnvironmentInstance(LogConfig.create(reader, writer));
    }

    protected VirtualFileSystem createVirtualFileSystem() {
        return new VirtualFileSystem(getEnvironment());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        vfs.shutdown();
        super.tearDown();
    }
}
