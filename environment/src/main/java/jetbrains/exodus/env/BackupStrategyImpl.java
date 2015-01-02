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
package jetbrains.exodus.env;

import jetbrains.exodus.BackupStrategy;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

class BackupStrategyImpl implements BackupStrategy {

    @NotNull
    private final EnvironmentImpl environment;

    public BackupStrategyImpl(@NotNull EnvironmentImpl environment) {
        this.environment = environment;
    }

    @Override
        public void beforeBackup() {
        environment.suspendGC();
        }

        @Override
        public Iterable<Pair<File, String>> listFiles() {
            return new Iterable<Pair<File, String>>() {
                final File[] files = IOUtil.listFiles(new File(environment.getLog().getLocation()));
                int i = 0;
                File current;

                @Override
                public Iterator<Pair<File, String>> iterator() {
                    return new Iterator<Pair<File, String>>() {

                        @Override
                        public boolean hasNext() {
                            if (current != null) {
                                return true;
                            }
                            while (i < files.length) {
                                final File next = files[i++];
                                if (next.isFile() && next.length() != 0 && next.getName().endsWith(LogUtil.LOG_FILE_EXTENSION)) {
                                    current = next;
                                    return true;
                                }
                            }
                            return false;
                        }

                        @Override
                        public Pair<File, String> next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            final Pair<File, String> result = new Pair<File, String>(current, "");
                            current = null;
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

        @Override
        public void onError(Throwable t) {
        }
}
