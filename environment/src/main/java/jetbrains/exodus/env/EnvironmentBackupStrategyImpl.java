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
package jetbrains.exodus.env;

import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

class EnvironmentBackupStrategyImpl extends BackupStrategy {

    @NotNull
    private final EnvironmentImpl environment;

    public EnvironmentBackupStrategyImpl(@NotNull EnvironmentImpl environment) {
        this.environment = environment;
    }

    @Override
    public void beforeBackup() {
        environment.suspendGC();
        environment.flushAndSync();
    }

    @Override
    public Iterable<VirtualFileDescriptor> getContents() {
        return new Iterable<VirtualFileDescriptor>() {

            private final File[] files = IOUtil.listFiles(new File(environment.getLog().getLocation()));
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
                                if (fileSize != 0 && file.getName().endsWith(LogUtil.LOG_FILE_EXTENSION)) {
                                    next = new FileDescriptor(file, "", fileSize);
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
