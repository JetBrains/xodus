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

import jetbrains.exodus.crypto.KryptKt;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import org.jetbrains.annotations.NotNull;

import java.io.File;

@SuppressWarnings("UnusedDeclaration")
public final class Environments {

    private Environments() {
    }

    @NotNull
    public static Environment newInstance(@NotNull final String dir, @NotNull final String subDir, @NotNull final EnvironmentConfig ec) {
        return prepare(new EnvironmentImpl(newLogInstance(new File(dir, subDir), ec), ec));
    }

    @NotNull
    public static Environment newInstance(@NotNull final String dir) {
        return newInstance(dir, new EnvironmentConfig());
    }

    @NotNull
    public static Environment newInstance(@NotNull final String dir, @NotNull final EnvironmentConfig ec) {
        return prepare(new EnvironmentImpl(newLogInstance(new File(dir), ec), ec));
    }

    @NotNull
    public static Environment newInstance(@NotNull final File dir) {
        return newInstance(dir, new EnvironmentConfig());
    }

    @NotNull
    public static Environment newInstance(@NotNull final File dir, @NotNull final EnvironmentConfig ec) {
        return prepare(new EnvironmentImpl(newLogInstance(dir, ec), ec));
    }

    @NotNull
    public static Environment newInstance(@NotNull final LogConfig config) {
        return newInstance(config, new EnvironmentConfig());
    }

    @NotNull
    public static Environment newInstance(@NotNull final LogConfig config, @NotNull final EnvironmentConfig ec) {
        return prepare(new EnvironmentImpl(newLogInstance(config, ec), ec));
    }

    @NotNull
    public static ContextualEnvironment newContextualInstance(@NotNull final String dir, @NotNull final String subDir, @NotNull final EnvironmentConfig ec) {
        return prepare(new ContextualEnvironmentImpl(newLogInstance(new File(dir, subDir), ec), ec));
    }

    @NotNull
    public static ContextualEnvironment newContextualInstance(@NotNull final String dir) {
        return newContextualInstance(dir, new EnvironmentConfig());
    }

    @NotNull
    public static ContextualEnvironment newContextualInstance(@NotNull final String dir, @NotNull final EnvironmentConfig ec) {
        return prepare(new ContextualEnvironmentImpl(newLogInstance(new File(dir), ec), ec));
    }

    @NotNull
    public static ContextualEnvironment newContextualInstance(@NotNull final File dir) {
        return newContextualInstance(dir, new EnvironmentConfig());
    }

    @NotNull
    public static ContextualEnvironment newContextualInstance(@NotNull final File dir, @NotNull final EnvironmentConfig ec) {
        return prepare(new ContextualEnvironmentImpl(newLogInstance(dir, ec), ec));
    }

    @NotNull
    public static ContextualEnvironment newContextualInstance(@NotNull final LogConfig config, @NotNull final EnvironmentConfig ec) {
        return prepare(new ContextualEnvironmentImpl(newLogInstance(config, ec), ec));
    }

    @NotNull
    public static Log newLogInstance(@NotNull final File dir, @NotNull final EnvironmentConfig ec) {
        return newLogInstance(new LogConfig().setDir(dir), ec);
    }

    @NotNull
    public static Log newLogInstance(@NotNull final LogConfig config, @NotNull final EnvironmentConfig ec) {
        final Long maxMemory = ec.getMemoryUsage();
        if (maxMemory != null) {
            config.setMemoryUsage(maxMemory);
        } else {
            config.setMemoryUsagePercentage(ec.getMemoryUsagePercentage());
        }
        return newLogInstance(config.setFileSize(ec.getLogFileSize()).
            setLockTimeout(ec.getLogLockTimeout()).
            setLockId(ec.getLogLockId()).
            setCachePageSize(ec.getLogCachePageSize()).
            setCacheOpenFilesCount(ec.getLogCacheOpenFilesCount()).
            setCacheUseNio(ec.getLogCacheUseNio()).
            setCacheFreePhysicalMemoryThreshold(ec.getLogCacheFreePhysicalMemoryThreshold()).
            setDurableWrite(ec.getLogDurableWrite()).
            setSharedCache(ec.isLogCacheShared()).
            setNonBlockingCache(ec.isLogCacheNonBlocking()).
            setCleanDirectoryExpected(ec.isLogCleanDirectoryExpected()).
            setClearInvalidLog(ec.isLogClearInvalid()).
            setSyncPeriod(ec.getLogSyncPeriod()).
            setFullFileReadonly(ec.isLogFullFileReadonly()).
            setCipherProvider(ec.getCipherId() == null ? null : KryptKt.newCipherProvider(ec.getCipherId())).
            setCipherKey(ec.getCipherKey()).
            setCipherBasicIV(ec.getCipherBasicIV()));
    }

    @NotNull
    public static Log newLogInstance(@NotNull final LogConfig config) {
        // In order to avoid XD-96, we need to load the DatabaseRoot class before creating Log instance
        return new Log(config);
    }

    @NotNull
    static <T extends EnvironmentImpl> T prepare(@NotNull final T env) {
        env.getGC().getUtilizationProfile().load();
        return env;
    }
}
