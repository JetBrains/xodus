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

import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

// TODO: incorporate it into the FileDataReader, but decouple Java 7 API usage because of Android
class FileSystemEnvironmentWatcher {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemEnvironmentWatcher.class);
    private static final long IDLE_FORCE_CHECK_INTERVAL = 3000L; // 3 seconds
    private static final long DEBOUNCE_INTERVAL = 100L; // 100 milliseconds

    @NotNull
    private final WatchService watchService;
    @NotNull
    private final WatchKey watchKey;
    @NotNull
    private final EnvironmentImpl env;
    // private final AtomicLong latestOperation = new AtomicLong();

    private volatile boolean stopped;

    FileSystemEnvironmentWatcher(@NotNull final File directory, @NotNull final EnvironmentImpl env) throws IOException {
        this.env = env;
        watchService = FileSystems.getDefault().newWatchService(); // TODO: make global?
        watchKey = directory.toPath().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
        new Thread(new Runnable() {
            @Override
            public void run() {
                doWatch();
            }
        }).start();
    }

    void stop() {
        stopped = true;
        watchKey.cancel();
        try {
            watchService.close();
        } catch (IOException ignore) {
        }
    }

    private void doWatch() {
        long lastDirty = Long.MIN_VALUE;
        while (!stopped) {
            final WatchKey watchKey;
            boolean hasFileUpdates = false;
            try {
                watchKey = watchService.poll(45, TimeUnit.MILLISECONDS);
                final List<WatchEvent<?>> events;
                if (watchKey == null || (events = watchKey.pollEvents()).isEmpty()) {
                    if (lastDirty > Long.MIN_VALUE && System.currentTimeMillis() - lastDirty > IDLE_FORCE_CHECK_INTERVAL) {
                        lastDirty = doUpdate(true);
                    }
                    continue;
                }
                for (final WatchEvent<?> event : events) {
                    final Object eventContext = event.context();
                    if (eventContext instanceof Path && LogUtil.LOG_FILE_NAME_FILTER.accept(null, ((Path) eventContext).getFileName().toString())) {
                        hasFileUpdates = true;
                        break;
                    }
                }
            } catch (final InterruptedException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("File watcher interrupted", e);
                }
                Thread.currentThread().interrupt();
                return;
            } catch (ClosedWatchServiceException ignore) {
                return;
            }
            if (lastDirty > Long.MIN_VALUE) {
                final long debounce = DEBOUNCE_INTERVAL + (lastDirty - System.currentTimeMillis());
                if (debounce > 5) {
                    try {
                        Thread.sleep(debounce);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            if (hasFileUpdates) {
                lastDirty = doUpdate(false);
            }
            if (!watchKey.reset()) {
                return;
            }
        }
    }

    private long doUpdate(final boolean force) {
        // latestOperation.incrementAndGet();
        if (UnsafeKt.tryUpdate(env)) {
            if (logger.isInfoEnabled()) {
                logger.info((force ? "Env force-updated at " : "Env updated at ") + env.getLocation());
            }
            return Long.MIN_VALUE;
        }
        if (logger.isInfoEnabled()) {
            logger.info((force ? "Can't force-update env at " : "Can't update env at ") + env.getLocation());
        }
        return System.currentTimeMillis();
    }
}
