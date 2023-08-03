/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.OutOfDiskSpaceException;
import jetbrains.exodus.system.JVMConstants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Scanner;

public final class LockingManager {
    public static final String LOCK_FILE_NAME = "xd.lck";

    private final File dir;
    private final String lockId;

    private RandomAccessFile lockFile;
    private FileLock lock;


    public LockingManager(File dir, String lockId) {
        this.dir = dir;
        this.lockId = lockId;
    }


    public long getUsableSpace() {
        return this.dir.getUsableSpace();
    }

    public boolean lock(long timeout) {
        var started = System.currentTimeMillis();
        do {
            if (lock()) {
                return true;
            }
            if (timeout > 0) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } while (System.currentTimeMillis() - started < timeout);
        return false;
    }

    public boolean release() {
        if (lockFile != null) {
            try {
                close();
                return true;
            } catch (IOException e) {
                throw new ExodusException("Failed to release lock file " + LOCK_FILE_NAME, e);
            }
        }
        return false;
    }

    private boolean lock() {
        if (lockFile != null) {
            return false; // already locked!
        }
        try {
            var lockFileHandle = getLockFile();
            var lockFile = new RandomAccessFile(lockFileHandle, "rw");
            this.lockFile = lockFile;
            var channel = lockFile.getChannel();
            lock = channel.tryLock();
            if (lock != null) {
                lockFile.setLength(0);
                lockFile.writeBytes("Private property of Exodus: ");
                if (lockId == null) {
                    if (!JVMConstants.getIS_ANDROID()) {
                        var bean = ManagementFactory.getRuntimeMXBean();
                        if (bean != null) {
                            // Got runtime system bean (try to get PID)
                            // Result of bean.getName() is unknown
                            lockFile.writeBytes(bean.getName());
                        }
                    }
                } else {
                    lockFile.writeBytes(lockId);
                }
                lockFile.writeBytes("\n\n");
                for (var element : new Throwable().getStackTrace()) {
                    lockFile.writeBytes(element.toString() + '\n');
                }
                channel.force(false);
            }
        } catch (IOException e) {
            try {
                close();
            } catch (IOException ioe) {
                //throw only first cause
            }
            return throwFailedToLock(e);
        } catch (OverlappingFileLockException e) {
            try {
                close();
            } catch (IOException ioe) {
                //throw only first cause
            }
        }
        if (lock == null) {
            try {
                close();
            } catch (IOException e) {
                throwFailedToLock(e);
            }
        }
        return lockFile != null;
    }

    private File getLockFile() {
        return new File(dir, LOCK_FILE_NAME);
    }

    public String lockFilePath() {
        return getLockFile().getAbsolutePath();
    }

    public String lockInfo() {
        try {
            // "stupid scanner trick" for reading entire file in a string (https://community.oracle.com/blogs/pat/2004/10/23/stupid-scanner-tricks)
            var scanner = new Scanner(getLockFile()).useDelimiter("\\A");
            if (scanner.hasNext()) {
                return scanner.next();
            }

            return null;
        } catch (IOException e) {
            throw new ExodusException("Failed to read contents of lock file " + LOCK_FILE_NAME, e);
        }
    }

    private boolean throwFailedToLock(IOException e) {
        if (getUsableSpace() < 4096) {
            throw new OutOfDiskSpaceException(e);
        }
        throw new ExodusException("Failed to lock file " + LOCK_FILE_NAME, e);
    }

    private void close() throws IOException {
        if (lock != null) {
            lock.release();
        }

        if (lockFile != null) {
            lockFile.close();
            lockFile = null;
        }
    }
}
