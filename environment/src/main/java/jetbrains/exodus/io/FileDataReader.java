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
package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.util.SharedRandomAccessFile;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

@SuppressWarnings({"PackageVisibleField", "ClassEscapesDefinedScope"})
public class FileDataReader implements DataReader {

    private static final Logger logger = LoggerFactory.getLogger(FileDataReader.class);

    private static final long DEFAULT_FREE_PHYSICAL_MEMORY_THRESHOLD = 1_000_000_000L; // ~1GB
    private static final String DELETED_FILE_EXTENSION = ".del";

    @NotNull
    private final File dir;
    private final boolean useNio;

    public FileDataReader(@NotNull final File dir, final int openFiles) {
        this(dir, openFiles, true, DEFAULT_FREE_PHYSICAL_MEMORY_THRESHOLD);
    }

    public FileDataReader(@NotNull final File dir,
                          final int openFiles,
                          final boolean useNio,
                          final long freePhysicalMemoryThreshold) {
        this.dir = dir;
        this.useNio = useNio;
        SharedOpenFilesCache.setSize(openFiles);
        if (useNio) {
            SharedMappedFilesCache.setFreePhysicalMemoryThreshold(freePhysicalMemoryThreshold);
        }
    }

    @Override
    public Block[] getBlocks() {
        final File[] files = LogUtil.listFiles(dir);
        final Block[] result = new Block[files.length];
        for (int i = 0; i < files.length; ++i) {
            result[i] = getBlock(LogUtil.getAddress(files[i].getName()));
        }
        sortBlocks(result);
        return result;
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
        } catch (IOException e) {
            throw new ExodusException("Can't close all files", e);
        }
    }

    @Override
    public String getLocation() {
        return dir.getPath();
    }

    @Override
    public FileBlock getBlock(final long address) {
        return new FileBlock(address);
    }

    public static void sortBlocks(Block[] result) {
        Arrays.sort(result, new Comparator<Block>() {
            @Override
            public int compare(Block o1, Block o2) {
                if (o1.getAddress() < o2.getAddress()) {
                    return -1;
                }
                if (o1.getAddress() > o2.getAddress()) {
                    return 1;
                }
                return 0;
            }
        });
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
                            !canWrite() /* only read-only (immutable) files can be mapped */) {
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
