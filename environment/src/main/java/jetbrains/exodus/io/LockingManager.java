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
package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * Holds reference to lock files. Provides proper locking Exodus lock file & releasing.
 */
public class LockingManager {

    private static final String LOCK_FILE_NAME = "xd.lck";

    @NotNull
    private final File dir;
    @Nullable
    private RandomAccessFile lockFile;
    @Nullable
    private FileLock lock;
    @Nullable
    private FileChannel channel;
    @Nullable
    private File lockFileHandle;

    LockingManager(@NotNull File dir) {
        this.dir = dir;
    }

    public boolean lock() {
        if (lockFile != null) return false; // already locked!
        try {
            final File lockFileHandle = new File(dir, LOCK_FILE_NAME);
            final RandomAccessFile lockFile = new RandomAccessFile(lockFileHandle, "rw");
            this.lockFileHandle = lockFileHandle;
            this.lockFile = lockFile;
            final FileChannel channel = lockFile.getChannel();
            this.channel = channel;
            lock = channel.tryLock();
            if (lock != null) {
                lockFile.setLength(0);
                RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
                if (bean != null) {
                    // Got runtime system bean (try to get PID)
                    // Result of bean.getName() is unknown
                    lockFile.writeUTF(bean.getName() + ": ");
                }
                lockFile.writeUTF("private property of Exodus");
                channel.force(false);
            }
        } catch (IOException e) {
            try {
                close();
            } catch (IOException ex) {
                //throw only first cause
            }
            throw new ExodusException("Failed to lock file " + LOCK_FILE_NAME, e);
        } catch (OverlappingFileLockException ofle) {
            try {
                close();
            } catch (IOException ex) {
                //throw only first cause
            }
        }
        if (lock == null) {
            try {
                close();
            } catch (IOException e) {
                throw new ExodusException("Failed to lock file " + LOCK_FILE_NAME, e);
            }
        }
        return lockFile != null;
    }

    public boolean lock(final long timeout) {
        final long started = System.currentTimeMillis();
        boolean result;
        do {
            if ((result = lock())) break;
            if (timeout > 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        while (System.currentTimeMillis() - started < timeout);
        return result;
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

    public long getUsableSpace() {
        final File handle = lockFileHandle;
        if (handle == null) {
            throw new IllegalStateException("Lock file " + LOCK_FILE_NAME + " should be acquired to know the free space");
        }
        return handle.getUsableSpace();
    }

    /**
     * Gently closes lock-file.
     *
     * @throws IOException if close is failed
     */
    private void close() throws IOException {
        if (lock != null) lock.release();
        if (channel != null) channel.close();
        if (lockFile != null) {
            lockFile.close();
            lockFile = null;
        }
        if (lockFileHandle != null) {
            lockFileHandle = null;
        }
    }
}
