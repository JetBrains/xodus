/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

class EnvironmentBackupStrategyImpl extends BackupStrategy {

    @NotNull
    private final EnvironmentImpl environment;
    private long rootEndAddress;
    private final int pageSize;

    public EnvironmentBackupStrategyImpl(@NotNull EnvironmentImpl environment) {
        this.environment = environment;
        pageSize = environment.getEnvironmentConfig().getLogCachePageSize();
    }


    @Override
    public void beforeBackup() {
        environment.suspendGC();

        rootEndAddress = environment.computeInReadonlyTransaction(txn -> {
            final long address = ((TransactionBase) txn).getRoot();
            final Loggable loggable = environment.getLog().read(address);

            return address + loggable.length();
        });
    }

    @Override
    public Iterable<VirtualFileDescriptor> getContents() {
        environment.flushAndSync();

        return new Iterable<VirtualFileDescriptor>() {

            private final File[] files = IOUtil.listFiles(new File(environment.getLocation()));
            private int i = 0;
            private VirtualFileDescriptor next;

            @NotNull
            @Override
            public Iterator<VirtualFileDescriptor> iterator() {
                return new Iterator<VirtualFileDescriptor>() {

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

                                    if (rootEndAddress <= fileAddress) {
                                        return false;
                                    }


                                    long updatedFileSize = Math.min(fileSize, rootEndAddress - fileAddress);
                                    updatedFileSize = ((updatedFileSize + pageSize - 1) / pageSize) * pageSize;

                                    next = new FileDescriptor(file, "", updatedFileSize) {
                                        @Override
                                        public @NotNull InputStream getInputStream() throws IOException {
                                            return new FileDescriptorInputStream(new FileInputStream(file),
                                                    fileAddress, pageSize, getFileSize(), rootEndAddress - fileAddress,
                                                    environment.getCipherProvider(),
                                                    environment.getCipherKey(), environment.getCipherBasicIV());
                                        }
                                    };

                                    return true;
                                }
                            }
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
