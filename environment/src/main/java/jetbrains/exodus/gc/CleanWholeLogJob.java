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
package jetbrains.exodus.gc;

import jetbrains.exodus.core.execution.LatchJob;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CleanWholeLogJob extends LatchJob {

    private static final Logger logger = LoggerFactory.getLogger(CleanWholeLogJob.class);

    @NotNull
    private final GarbageCollector gc;

    CleanWholeLogJob(@NotNull GarbageCollector gc) {
        this.gc = gc;
    }

    @Override
    protected void execute() throws Throwable {
        info("CleanWholeLogJob started");
        try {
            final Log log = gc.getLog();
            long lastNumberOfFiles = Long.MAX_VALUE;
            long numberOfFiles;
            // repeat cleaning until number of files stops decreasing
            while ((numberOfFiles = log.getNumberOfFiles()) != 1 && numberOfFiles < lastNumberOfFiles) {
                lastNumberOfFiles = numberOfFiles;
                final long highFileAddress = log.getHighFileAddress();
                long fileAddress = log.getLowAddress();
                while (fileAddress != highFileAddress) {
                    gc.doCleanFile(fileAddress);
                    fileAddress = log.getNextFileAddress(fileAddress);
                }
                gc.testDeletePendingFiles();
            }
        } finally {
            release();
            info("CleanWholeLogJob finished");
        }
    }

    private static void info(@NotNull final String message) {
        if (logger.isInfoEnabled()) {
            logger.info(message);
        }
    }
}

