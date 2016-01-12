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
package jetbrains.exodus.env;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.ThreadJobProcessor;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.LogConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class EnvironmentLockTest extends EnvironmentTestsBase {

    @Test
    public void testAlreadyLockedEnvironment() throws IOException {
        boolean exOnCreate = false;
        Environment first = null;
        try {
            final File dir = getEnvDirectory();
            first = Environments.newInstance(LogConfig.create(new FileDataReader(dir, 16), new FileDataWriter(dir)), EnvironmentConfig.DEFAULT);
        } catch (ExodusException ex) {
            //Environment already created on startup!
            exOnCreate = true;
        }
        if (first != null) first.close();
        Assert.assertTrue(exOnCreate);
    }

    private final boolean[] wasOpened = new boolean[1];
    private final JobProcessor processor = new ThreadJobProcessor("EnvironmentLockTest");

    @Test
    public void testWaitForLockedEnvironment() throws InterruptedException {
        wasOpened[0] = false;
        openConcurrentEnvironment();
        closeEnvironment();
        Thread.sleep(1000);
        Assert.assertTrue(wasOpened[0]);
    }

    @Test
    public void testWaitForLockedEnvironment2() throws InterruptedException {
        wasOpened[0] = false;
        openConcurrentEnvironment();
        Thread.sleep(1000);
        closeEnvironment();
        Thread.sleep(1000);
        Assert.assertTrue(wasOpened[0]);
    }

    @Test
    public void testWaitForLockedEnvironment3() throws InterruptedException {
        wasOpened[0] = false;
        openConcurrentEnvironment();
        Thread.sleep(6000);
        closeEnvironment();
        Thread.sleep(1000);
        Assert.assertFalse(wasOpened[0]);
    }

    private void openConcurrentEnvironment() {
        processor.start();
        new Job(processor) {
            @Override
            protected void execute() throws Throwable {
                final File dir = getEnvDirectory();
                try {
                    env = newEnvironmentInstance(LogConfig.create(new FileDataReader(dir, 16), new FileDataWriter(dir)), new EnvironmentConfig().setLogLockTimeout(5000));
                    wasOpened[0] = true;
                } catch (ExodusException e) {
                    wasOpened[0] = false;
                }
            }
        };
    }

    @Override
    protected File getEnvDirectory() {
        final File dir = super.getEnvDirectory();
        return new File(dir.getParentFile(), dir.getName());
    }

    private void closeEnvironment() {
        if (env != null) {
            env.close();
            env = null;
        }
    }
}
