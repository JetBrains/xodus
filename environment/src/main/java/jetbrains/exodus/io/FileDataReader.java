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
import jetbrains.exodus.core.dataStructures.LongObjectCache;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.util.SharedRandomAccessFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

@SuppressWarnings({"PackageVisibleField", "ClassEscapesDefinedScope"})
public class FileDataReader implements DataReader {

    private static final Log logging = LogFactory.getLog(FileDataReader.class);

    private static final String DELETED_FILE_EXTENSION = ".del";

    @NotNull
    private final File dir;
    @NotNull
    private final LongObjectCache<SharedRandomAccessFile> fileCache;

    public FileDataReader(@NotNull final File dir, final int openFiles) {
        this.dir = dir;
        fileCache = new LongObjectCache<SharedRandomAccessFile>(openFiles);
    }

    @Override
    public Block[] getBlocks() {
        final File[] files = LogUtil.listFiles(dir);
        final Block[] result = new Block[files.length];
        for (int i = 0; i < files.length; ++i) {
            result[i] = new FileBlock(LogUtil.getAddress(files[i].getName()));
        }
        sortBlocks(result);
        return result;
    }

    @Override
    public void removeBlock(long blockAddress, @NotNull final RemoveBlockType rbt) {
        fileCache.lock();
        final SharedRandomAccessFile f;
        try {
            f = fileCache.remove(blockAddress);
        } finally {
            fileCache.unlock();
        }
        try {
            if (f != null) {
                f.close();
            }
        } catch (IOException e) {
            throw new ExodusException(e);
        }
        final File file = new File(dir, LogUtil.getLogFilename(blockAddress));
        final boolean deleted = rbt == RemoveBlockType.Delete ? file.delete() : renameFile(file);
        if (!deleted) {
            throw new ExodusException("Failed to delete " + file.getAbsolutePath());
        } else if (logging.isInfoEnabled()) {
            logging.info("Deleted file " + file.getAbsolutePath());
        }
    }

    @Override
    public void clear() {
        close();
        for (final File file : LogUtil.listFiles(dir)) {
            if (!file.delete()) {
                throw new ExodusException("Failed to delete " + file);
            }
        }
    }

    @Override
    public void close() {
        try {
            final Iterator<SharedRandomAccessFile> itr = fileCache.values();
            while (itr.hasNext()) {
                itr.next().close();
            }
        } catch (IOException e) {
            throw new ExodusException("Can't close all files", e);
        } finally {
            fileCache.clear();
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

    private static boolean renameFile(@NotNull final File file) {
        final String name = file.getName();
        return file.renameTo(new File(file.getParent(),
                name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION));
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
                fileCache.lock();
                SharedRandomAccessFile f;
                try {
                    f = fileCache.tryKey(address);
                    if (f != null && f.employ() > 1) {
                        f.close();
                        f = null;
                    }
                } finally {
                    fileCache.unlock();
                }
                if (f == null) {
                    f = new SharedRandomAccessFile(this, "r");
                    SharedRandomAccessFile obsolete = null;
                    fileCache.lock();
                    try {
                        if (fileCache.getObject(address) == null) {
                            f.employ();
                            obsolete = fileCache.cacheObject(address, f);
                        }
                    } finally {
                        fileCache.unlock();
                        if (obsolete != null) {
                            obsolete.close();
                        }
                    }
                }
                try {
                    f.seek(position);
                    return f.read(output, 0, count);
                } finally {
                    f.close();
                }
            } catch (IOException e) {
                throw new ExodusException("Can't read file " + getAbsolutePath(), e);
            }
        }
    }
}
