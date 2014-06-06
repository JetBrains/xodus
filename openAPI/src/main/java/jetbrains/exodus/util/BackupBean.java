/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.util;

import jetbrains.exodus.BackupStrategy;
import jetbrains.exodus.Backupable;
import jetbrains.exodus.core.dataStructures.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public class BackupBean implements Backupable {

    private static final Log log = LogFactory.getLog(BackupBean.class);

    private final Backupable target;
    private volatile long backupStartTicks;
    private String backupPath;
    private boolean backupToZip;
    private String backupNamePrefix;
    private String commandAfterBackup;
    private Throwable backupException;

    public BackupBean(final Backupable target) {
        this.target = target;
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

    public void setBackupStartTicks(long backupStartTicks) {
        this.backupStartTicks = backupStartTicks;
    }

    public long getBackupStartTicks() {
        return backupStartTicks;
    }

    public void setBackupException(Throwable backupException) {
        this.backupException = backupException;
    }

    public Throwable getBackupException() {
        return backupException;
    }

    @Override
    public BackupStrategy getBackupStrategy() {
        final BackupStrategy wrapped = target.getBackupStrategy();
        return new BackupStrategy() {
            @Override
            public void beforeBackup() throws Exception {
                backupStartTicks = System.currentTimeMillis();
                log.info("Backing up database...");
                wrapped.beforeBackup();
            }

            @Override
            public Iterable<Pair<File, String>> listFiles() {
                return wrapped.listFiles();
            }

            @Override
            public void afterBackup() throws Exception {
                try {
                    wrapped.afterBackup();
                } finally {
                    backupStartTicks = 0;
                }
                if (commandAfterBackup != null) {
                    log.info("Executing \"" + commandAfterBackup + "\"...");
                    //noinspection CallToRuntimeExecWithNonConstantString,CallToRuntimeExec
                    Runtime.getRuntime().exec(commandAfterBackup);
                }
                log.info("Backup finished.");
            }

            @Override
            public void onError(Throwable t) {
                backupException = t;
            }
        };
    }
}
