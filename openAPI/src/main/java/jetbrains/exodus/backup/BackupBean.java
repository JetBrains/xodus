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
package jetbrains.exodus.backup;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * {@code BackupBean} is a {@linkplain Backupable} decorator of one or several {@code Backupables}.
 * In an application working with an {@linkplain jetbrains.exodus.env.Environment}, using {@code BackupBean} can look
 * as the following:
 * <pre>
 *     final BackupBean backupBean = new BackupBean(environment);
 *     backupBean.setBackupToZip(true);
 *     backupBean.setBackupPath(new File(environment.getLocation(), "backups").getAbsolutePath());
 *     backupBean.setBackupNamePrefix(new Date().toString());
 *     // ...
 *     // ...
 *     // and further
 *     final File backup = CompressBackupUtil.backup(backupBean);
 * </pre>
 *
 * @see Backupable
 */
public class BackupBean implements Backupable {

    private static final Logger logger = LoggerFactory.getLogger(BackupBean.class);

    private final Backupable[] targets;
    private volatile long backupStartTicks;
    private String backupPath;
    private boolean backupToZip;
    private String backupNamePrefix;
    private String commandAfterBackup;
    private List<Runnable> runAfterBackup = new ArrayList<>();
    private Throwable backupException;

    public BackupBean(final Backupable target) {
        targets = new Backupable[]{target};
    }

    public BackupBean(final List<Backupable> targets) {
        final int targetsCount = targets.size();
        if (targetsCount < 1) {
            throw new IllegalArgumentException();
        }
        this.targets = targets.toArray(new Backupable[targetsCount]);
    }

    public void setBackupPath(@NotNull final String backupPath) {
        this.backupPath = backupPath;
    }

    public String getBackupPath() {
        return backupPath;
    }

    public boolean getBackupToZip() {
        return backupToZip;
    }

    public void setBackupToZip(boolean zip) {
        backupToZip = zip;
    }

    public String getBackupNamePrefix() {
        return backupNamePrefix;
    }

    public void setBackupNamePrefix(String prefix) {
        backupNamePrefix = prefix;
    }

    public void setCommandAfterBackup(@Nullable final String command) {
        commandAfterBackup = command;
    }

    public String getCommandAfterBackup() {
        return commandAfterBackup;
    }

    public void executeAfterBackup(@NotNull final Runnable runnable) {
        runAfterBackup.add(runnable);
    }

    /**
     * Sets time when backup started.
     *
     * @param backupStartTicks time when backup started, 0 if backup is finished
     */
    public void setBackupStartTicks(long backupStartTicks) {
        this.backupStartTicks = backupStartTicks;
    }

    public long getBackupStartTicks() {
        return backupStartTicks;
    }

    public boolean isBackupInProgress() {
        return backupStartTicks > 0L;
    }

    public void setBackupException(Throwable backupException) {
        this.backupException = backupException;
    }

    public Throwable getBackupException() {
        return backupException;
    }

    @NotNull
    @Override
    public BackupStrategy getBackupStrategy() {
        final int targetsCount = targets.length;
        final BackupStrategy[] wrapped = new BackupStrategy[targetsCount];
        for (int i = 0; i < targetsCount; i++) {
            wrapped[i] = targets[i].getBackupStrategy();
        }
        return new BackupStrategy() {
            @Override
            public void beforeBackup() throws Exception {
                backupStartTicks = System.currentTimeMillis();
                logger.info("Backing up database...");
                for (final BackupStrategy strategy : wrapped) {
                    strategy.beforeBackup();
                }
            }

            @Override
            public Iterable<VirtualFileDescriptor> getContents() {
                return new Iterable<VirtualFileDescriptor>() {
                    @NotNull
                    @Override
                    public Iterator<VirtualFileDescriptor> iterator() {
                        return new Iterator<VirtualFileDescriptor>() {

                            @Nullable
                            private VirtualFileDescriptor next = null;
                            private int i = 0;
                            @NotNull
                            private Iterator<VirtualFileDescriptor> it = EMPTY.getContents().iterator();

                            @Override
                            public boolean hasNext() {
                                return getNext() != null;
                            }

                            @Override
                            public VirtualFileDescriptor next() {
                                try {
                                    return getNext();
                                } finally {
                                    next = null;
                                }
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException("remove");
                            }

                            private VirtualFileDescriptor getNext() {
                                if (next == null) {
                                    while (!it.hasNext()) {
                                        if (i >= targetsCount) {
                                            return next;
                                        }
                                        it = wrapped[i++].getContents().iterator();
                                    }
                                    next = it.next();
                                }
                                final long acceptedSize = wrapped[i - 1].acceptFile(next);
                                if (acceptedSize < next.getFileSize()) {
                                    return next.copy(acceptedSize);
                                }
                                return next;
                            }
                        };
                    }
                };
            }

            @Override
            public void afterBackup() throws Exception {
                try {
                    for (final BackupStrategy strategy : wrapped) {
                        strategy.afterBackup();
                    }
                } finally {
                    backupStartTicks = 0;
                }
                for (final Runnable runnable : runAfterBackup) {
                    runnable.run();
                }
                if (commandAfterBackup != null) {
                    logger.info("Executing \"" + commandAfterBackup + "\"...");
                    //noinspection CallToRuntimeExecWithNonConstantString,CallToRuntimeExec
                    Runtime.getRuntime().exec(commandAfterBackup);
                }
                logger.info("Backup finished.");
            }

            @Override
            public void onError(Throwable t) {
                backupException = t;
            }
        };
    }
}
