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
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.util.SharedRandomAccessFile;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public final class FileDataReader implements DataReader {

    private final File dir;
    private Log log;
    boolean usedWithWatcher = false;

    public FileDataReader(File dir) {
        this.dir = dir;
    }

    public File getDir() {
        return dir;
    }

    @Override
    @NotNull
    public Iterable<Block> getBlocks() {
        var files = LogUtil.listFileAddresses(dir);
        files.sort();
        return toBlocks(files);
    }

    @Override
    @NotNull
    public Iterable<Block> getBlocks(long fromAddress) {
        var files = LogUtil.listFileAddresses(fromAddress, dir);
        files.sort();
        return toBlocks(files);
    }

    public void close() {
        try {
            SharedOpenFilesCache.getInstance().removeDirectory(dir);
        } catch (IOException e) {
            throw new ExodusException("Can't close all files", e);
        }
    }

    public void setLog(Log log) {
        this.log = log;
    }

    @Override
    @NotNull
    public String getLocation() {
        return dir.getPath();
    }


    private ArrayList<Block> toBlocks(LongArrayList files) {
        var blocks = new ArrayList<Block>(files.size());
        for (var i = 0; i < files.size(); i++) {
            blocks.add(new FileBlock(files.get(i), this));
        }

        return blocks;
    }

    public static final class FileBlock extends File implements Block {
        private final long address;
        private final FileDataReader reader;

        FileBlock(long address, FileDataReader reader) {
            super(reader.dir, LogUtil.getLogFilename(address));
            this.address = address;
            this.reader = reader;
        }

        @Override
        public long getAddress() {
            return address;
        }

        @Override
        public int read(byte[] output, long position, int offset, int count) {
            try {
                var log = reader.log;
                boolean immutable;

                if (log != null) {
                    immutable = log.isImmutableFile(address);
                } else {
                    immutable = !canWrite();
                }

                var filesCache = SharedOpenFilesCache.getInstance();
                SharedRandomAccessFile file;
                if (immutable && !reader.usedWithWatcher) {
                    file = filesCache.getCachedFile(this);
                } else {
                    file = filesCache.openFile(this);
                }

                try (file) {
                    file.seek(position);

                    return readFully(file, output, offset, count);
                }
            } catch (IOException e) {
                throw new ExodusException("Can't read file " + getAbsolutePath(), e);
            }
        }

        private int readFully(RandomAccessFile file, byte[] output, int offset, int size) throws IOException {
            var read = 0;

            while (read < size) {
                var r = file.read(output, offset + read, size - read);
                if (r == -1) {
                    break;
                }
                read += r;
            }

            return read;
        }

        @Override
        public Block refresh() {
            return this;
        }
    }
}
