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
package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.OutOfDiskSpaceException;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

public class FileDataWriter extends AbstractDataWriter {

    private static final Logger logger = LoggerFactory.getLogger(FileDataWriter.class);

    @NotNull
    private final File dir;
    private final FileChannel dirChannel;
    @NotNull
    private final LockingManager lockingManager;
    @Nullable
    private RandomAccessFile file;

    public FileDataWriter(@NotNull final File directory) {
        this(directory, null);
    }

    public FileDataWriter(@NotNull File directory, @Nullable String lockId) {
        file = null;
        dir = directory;
        FileChannel channel = null;
        try {
            channel = FileChannel.open(dir.toPath());
        } catch (IOException e) {
            logger.warn("Can't open directory channel. Log directory fsync won't be performed.");
        }
        dirChannel = channel;
        lockingManager = new LockingManager(dir, lockId);
    }

    @Override
    public void write(byte[] b, int off, int len) throws ExodusException {
        final RandomAccessFile file = this.file;
        if (file == null) {
            throw new ExodusException("Can't write, FileDataWriter is closed");
        }
        try {
            file.write(b, off, len);
        } catch (IOException ioe) {
            if (lockingManager.getUsableSpace() < len) {
                throw new OutOfDiskSpaceException(ioe);
            }
            throw new ExodusException("Can't write", ioe);
        }
    }

    @Override
    public boolean lock(long timeout) {
        return lockingManager.lock(timeout);
    }

    @Override
    public boolean release() {
        return lockingManager.release();
    }

    @Override
    public String lockInfo() {
        return lockingManager.lockInfo();
    }

    @Override
    protected void syncImpl() {
        final RandomAccessFile file = this.file;
        if (file != null) {
            forceSync(file);
        }
    }

    @Override
    protected void closeImpl() {
        final RandomAccessFile file = this.file;
        if (file == null) {
            throw new ExodusException("Can't close already closed FileDataWriter");
        }
        try {
            file.close();
            this.file = null;
        } catch (IOException e) {
            throw new ExodusException("Can't close FileDataWriter", e);
        }
    }

    @Override
    protected void openOrCreateBlockImpl(final long address, final long length) {
        try {
            final RandomAccessFile result = new RandomAccessFile(new File(dir, LogUtil.getLogFilename(address)), "rw");
            result.seek(length);
            if (length != result.length()) {
                result.setLength(length);
                forceSync(result);
            }
            file = result;
        } catch (IOException ioe) {
            throw new ExodusException(ioe);
        }
    }

    private static void forceSync(@NotNull final RandomAccessFile file) {
        try {
            final FileChannel channel = file.getChannel();
            channel.force(false);
        } catch (ClosedChannelException e) {
            // ignore
        } catch (IOException ioe) {
            if (file.getChannel().isOpen()) {
                throw new ExodusException(ioe);
            }
        }
    }

    @Override
    public void syncDirectory() {
        if (dirChannel != null) {
            try {
                dirChannel.force(false);
            } catch (IOException e) {
                throw new ExodusException("Cannot fsync directory", e);
            }
        }
    }
}
