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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

// TODO: incorporate it into the FileDataReader, but decouple Java 7 API usage because of Android
class FileSystemEnvironmentWatcher {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemEnvironmentWatcher.class);

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
        while (!stopped) {
            final WatchKey watchKey;
            try {
                watchKey = watchService.poll(20, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("File watcher interrupted", e);
                }
                Thread.currentThread().interrupt();
                return;
            } catch (ClosedWatchServiceException ignore) {
                return;
            }
            for (final WatchEvent<?> event : watchKey.pollEvents()) {
                final Object eventContext = event.context();
                if (eventContext instanceof Path) {
                    // latestOperation.incrementAndGet();
                    if (UnsafeKt.tryUpdate(env)) {
                        logger.info("Env updated");
                    } else {
                        logger.warn("Can't update env");
                    }
                }
            }
            if (!watchKey.reset()) {
                return;
            }
        }
    }
}
