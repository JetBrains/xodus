/**
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
package jetbrains.exodus.env;

import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.FileDescriptorInputStream;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import jetbrains.exodus.log.DataCorruptionException;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.StartupMetadata;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

class EnvironmentBackupStrategyImpl extends BackupStrategy {
    @NotNull
    private final EnvironmentImpl environment;

    private long rootEndAddress;
    private long fileLastAddress;
    private long fileLengthBound;
    private long rootAddress;
    private final int pageSize;
    private boolean startupMetadataWasSent;

    public EnvironmentBackupStrategyImpl(@NotNull EnvironmentImpl environment) {
        this.environment = environment;
        pageSize = environment.getEnvironmentConfig().getLogCachePageSize();
    }


    @Override
    public void beforeBackup() {
        environment.suspendGC();

        final long[] roots = environment.computeInReadonlyTransaction(txn -> {
            final long address = ((TransactionBase) txn).getRoot();
            final Loggable loggable = environment.getLog().read(address);

            return new long[]{address, loggable.end()};
        });

        rootAddress = roots[0];
        rootEndAddress = roots[1];

        fileLengthBound = environment.getLog().getFileLengthBound();
        fileLastAddress = (rootEndAddress / fileLengthBound) * fileLengthBound;
    }

    @Override
    public Iterable<VirtualFileDescriptor> getContents() {
        environment.flushAndSync();

        return new Iterable<>() {
            private final File[] files = IOUtil.listFiles(new File(environment.getLocation()));
            private int i = 0;
            private VirtualFileDescriptor next;

            @NotNull
            @Override
            public Iterator<VirtualFileDescriptor> iterator() {
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        if (next != null) {
                            return true;
                        }

                        while (i < files.length) {
                            final File file = files[i++];

                            if (file.isFile()) {
                                final long fileSize = file.length();
                                final String logFileName = file.getName();

                                if (fileSize != 0 && logFileName.endsWith(LogUtil.LOG_FILE_EXTENSION)) {
                                    final long fileAddress = LogUtil.getAddress(file.getName());

                                    if (fileLastAddress < fileAddress) {
                                        break;
                                    }

                                    if (fileLastAddress > fileAddress && fileSize < fileLengthBound) {
                                        DataCorruptionException.raise("Size of the file is less than expected. {expected : " +
                                                        fileLengthBound + ", actual : " + fileSize + " }",
                                                environment.getLog(), fileAddress);
                                    }

                                    long updatedFileSize = Math.min(fileSize, rootEndAddress - fileAddress);
                                    updatedFileSize = ((updatedFileSize + pageSize - 1) / pageSize) * pageSize;

                                    next = new FileDescriptor(file, "", updatedFileSize) {
                                        @Override
                                        public @NotNull InputStream getInputStream() throws IOException {
                                            return new FileDescriptorInputStream(new FileInputStream(file),
                                                    fileAddress, pageSize, getFileSize(),
                                                    rootEndAddress - fileAddress,
                                                    environment.getLog(), environment.getCipherProvider(),
                                                    environment.getCipherKey(), environment.getCipherBasicIV());
                                        }
                                    };

                                    return true;
                                }
                            }
                        }

                        if (!startupMetadataWasSent) {
                            startupMetadataWasSent = true;

                            final ByteBuffer metadataContent =
                                    StartupMetadata.serialize(0, environment.getCurrentFormatVersion(), rootAddress,
                                            environment.getLog().getCachePageSize(),
                                            environment.getLog().getFileLengthBound(),
                                            true);

                            next = new FileDescriptor(new File(StartupMetadata.FIRST_FILE_NAME),
                                    "", metadataContent.remaining()) {
                                @Override
                                public @NotNull InputStream getInputStream() {
                                    return new ByteArrayInputStream(metadataContent.array(),
                                            metadataContent.arrayOffset(), metadataContent.remaining());
                                }

                                @Override
                                public boolean shouldCloseStream() {
                                    return false;
                                }

                                @Override
                                public boolean hasContent() {
                                    return true;
                                }

                                @Override
                                public long getTimeStamp() {
                                    return System.currentTimeMillis();
                                }

                                @Override
                                public boolean canBeEncrypted() {
                                    return false;
                                }
                            };
                            return true;
                        }

                        return false;
                    }

                    @Override
                    public VirtualFileDescriptor next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        final VirtualFileDescriptor result = next;
                        next = null;
                        return result;
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
    public void afterBackup() {
        environment.resumeGC();
    }
}
