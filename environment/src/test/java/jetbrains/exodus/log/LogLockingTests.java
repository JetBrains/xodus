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
package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.io.DataWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class LogLockingTests extends LogTestsBase {

    @Test
    public void testLock() throws IOException {
        initLog(1);
        File xdLockFile = new File(getLogDirectory(), "xd.lck");
        Assert.assertTrue(xdLockFile.exists());
        Assert.assertFalse(canWrite(xdLockFile));
        closeLog();
        Assert.assertTrue(canWrite(xdLockFile));
    }

    @Test
    public void testLockContents() throws IOException {
        initLog(1);
        final DataWriter writer = log.getConfig().getWriter();
        closeLog();
        Assert.assertTrue(writer.lockInfo().contains("org.junit."));
    }

    @Test
    public void testDirectoryAlreadyLocked() throws IOException {
        initLog(1);
        File xdLockFile = new File(getLogDirectory(), "xd.lck");
        Assert.assertTrue(xdLockFile.exists());
        Log prevLog = log;
        boolean alreadyLockedEx = false;
        try {
            log = null;
            initLog(1);
        } catch (ExodusException ex) {
            alreadyLockedEx = true;
        }
        Assert.assertTrue(alreadyLockedEx);
        prevLog.close();
        closeLog();
    }

    @Test
    public void testDirectoryReleaseLock() throws IOException {
        initLog(1);
        File xdLockFile = new File(getLogDirectory(), "xd.lck");
        Assert.assertTrue(xdLockFile.exists());
        closeLog();
        xdLockFile = new File(getLogDirectory(), "xd.lck");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(xdLockFile));
        System.out.println(bufferedReader.readLine());
        bufferedReader.close();
        Assert.assertTrue(xdLockFile.exists());
        boolean alreadyLockedEx = false;
        try {
            initLog(1);
        } catch (ExodusException ex) {
            alreadyLockedEx = true;
        }
        Assert.assertFalse(alreadyLockedEx);
        closeLog();
    }

    private static boolean canWrite(File xdLockFile) throws IOException {
        boolean can = xdLockFile.canWrite();
        if (can) {
            FileOutputStream stream = null;
            try {
                stream = new FileOutputStream(xdLockFile);
                stream.write(42);
                stream.flush();
                stream.close();
            } catch (IOException ex) {
                // xdLockFile.canWrite() returns true, because of Java cannot recognize tha file is locked
                can = false;
            } finally {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e) {
                        can = false;
                    }
                }
            }
        }
        // If it didn't help for some reasons (saw that it doesn't work on Solaris 10)
        if (can) {
            RandomAccessFile file = null;
            FileChannel channel = null;
            try {
                file = new RandomAccessFile(xdLockFile, "rw");
                channel = file.getChannel();
                FileLock lock = channel.tryLock();
                if (lock != null) {
                    lock.release();
                } else {
                    can = false;
                }
                file.close();
            } catch (Throwable ignore) {
                can = false;
            } finally {
                if (channel != null) channel.close();
                if (file != null) file.close();
            }
        }
        return can;
    }

}
