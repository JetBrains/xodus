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
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.util.SharedRandomAccessFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

@SuppressWarnings({"PackageVisibleField", "ClassEscapesDefinedScope"})
public class FileDataReader implements DataReader {

    private static final Logger logger = LoggerFactory.getLogger(FileDataReader.class);

    private static final String DELETED_FILE_EXTENSION = ".del";
    
    @NotNull
    private final File dir;
    private final boolean useNio;
    @Nullable
    private Log log;

    public FileDataReader(@NotNull final File dir, final int openFiles) {
        this(dir, openFiles, true, EnvironmentConfig.DEFAULT.getLogCacheFreePhysicalMemoryThreshold());
    }

    public FileDataReader(@NotNull final File dir,
                          final int openFiles,
                          final boolean useNio,
                          final long freePhysicalMemoryThreshold) {
        this.dir = dir;
        this.useNio = useNio;
        SharedOpenFilesCache.setSize(openFiles);
        if (useNio) {
            SharedMappedFilesCache.createInstance(freePhysicalMemoryThreshold);
        }
    }

    @Override
    public Iterable<Block> getBlocks() {
        final LongArrayList files = LogUtil.listFileAddresses(dir);
        files.sort();
        return new Iterable<Block>() {
            @NotNull
            @Override
            public Iterator<Block> iterator() {
                return new Iterator<Block>() {
                    int index = 0;
                    final int size = files.size();

                    @Override
                    public boolean hasNext() {
                        return index < size;
                    }

                    @Override
                    public Block next() {
                        return getBlock(files.get(index++));
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public void removeBlock(long blockAddress, @NotNull final RemoveBlockType rbt) {
        final File file = new File(dir, LogUtil.getLogFilename(blockAddress));
        removeFileFromFileCache(file);
        setWritable(file);
        final boolean deleted = rbt == RemoveBlockType.Delete ? file.delete() : renameFile(file);
        if (!deleted) {
            throw new ExodusException("Failed to delete " + file.getAbsolutePath());
        } else if (logger.isInfoEnabled()) {
            logger.info("Deleted file " + file.getAbsolutePath());
        }
    }

    @Override
    public void truncateBlock(long blockAddress, long length) {
        final FileBlock file = getBlock(blockAddress);
        removeFileFromFileCache(file);
        setWritable(file);
        try {
            try (SharedRandomAccessFile f = new SharedRandomAccessFile(file, "rw")) {
                f.setLength(length);
            }
            if (logger.isInfoEnabled()) {
                logger.info("Truncated file " + file.getAbsolutePath() + " to length = " + length);
            }
        } catch (IOException e) {
            throw new ExodusException("Failed to truncate file " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public void clear() {
        close();
        for (final File file : LogUtil.listFiles(dir)) {
            if (!file.canWrite()) {
                setWritable(file);
            }
            if (file.exists() && !file.delete()) {
                throw new ExodusException("Failed to delete " + file);
            }
        }
    }

    @Override
    public void close() {
        try {
            SharedOpenFilesCache.getInstance().removeDirectory(dir);
            if (useNio) {
                SharedMappedFilesCache.getInstance().removeDirectory(dir);
            }
        } catch (IOException e) {
            throw new ExodusException("Can't close all files", e);
        }
    }

    @Override
    public void setLog(@NotNull Log log) {
        this.log = log;
    }

    @Override
    public String getLocation() {
        return dir.getPath();
    }

    @Override
    public FileBlock getBlock(final long address) {
        return new FileBlock(address);
    }

    private void removeFileFromFileCache(@NotNull final File file) {
        try {
            SharedOpenFilesCache.getInstance().removeFile(file);
            if (useNio) {
                SharedMappedFilesCache.getInstance().removeFileBuffer(file);
            }
        } catch (IOException e) {
            throw new ExodusException(e);
        }
    }

    private static boolean renameFile(@NotNull final File file) {
        final String name = file.getName();
        return file.renameTo(new File(file.getParent(),
                name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION));
    }

    private static void setWritable(@NotNull final File file) {
        if (file.exists() && !file.setWritable(true)) {
            throw new ExodusException("Failed to set writable " + file.getAbsolutePath());
        }
    }

    private final class FileBlock extends File implements Block {

        private final long address;

        private FileBlock(final long address) {
            super(dir, LogUtil.getLogFilename(address));
            this.address = address;
        }

        @Override
        public long getAddress() {
            return address;
        }

        @Override
        public int read(final byte[] output, long position, int count) {
            try {
                try (SharedRandomAccessFile f = SharedOpenFilesCache.getInstance().getCachedFile(this)) {
                    if (useNio &&
                            /* only read-only (immutable) files can be mapped */
                            ((log != null && log.isImmutableFile(address)) || (log == null && !canWrite()))) {
                        try {
                            try (SharedMappedByteBuffer mappedBuffer = SharedMappedFilesCache.getInstance().getFileBuffer(f)) {
                                final ByteBuffer buffer = mappedBuffer.getBuffer();
                                buffer.position((int) position);
                                buffer.get(output, 0, count);
                                return count;
                            }
                        } catch (Throwable t) {
                            // if we failed to read mapped file, then try ordinary RandomAccessFile.read()
                            if (logger.isWarnEnabled()) {
                                logger.warn("Failed to transfer bytes from memory mapped file", t);
                            }
                        }
                    }
                    f.seek(position);
                    return f.read(output, 0, count);
                }
            } catch (IOException e) {
                throw new ExodusException("Can't read file " + getAbsolutePath(), e);
            }
        }
    }
}
