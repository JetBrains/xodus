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
package jetbrains.exodus.gc

import jetbrains.exodus.core.execution.LatchJob
import mu.KLogging

internal class CleanEntireLogJob(private val gc: GarbageCollector) : LatchJob() {

    override fun execute() {
        logger.info { "CleanEntireLogJob started" }

        try {
            val log = gc.log
            var lastNumberOfFiles = java.lang.Long.MAX_VALUE
            // repeat cleaning until number of files stops decreasing
            while (true) {
                val numberOfFiles = log.numberOfFiles
                if (numberOfFiles == 1L || numberOfFiles >= lastNumberOfFiles) break
                lastNumberOfFiles = numberOfFiles
                val highFileAddress = log.highFileAddress
                var fileAddress = log.lowFileAddress
                while (fileAddress != highFileAddress) {
                    gc.doCleanFile(fileAddress)
                    fileAddress = log.getNextFileAddress(fileAddress)
                }
                gc.testDeletePendingFiles()
            }
        } finally {
            release()
            logger.info { "CleanEntireLogJob finished" }
        }
    }

    companion object : KLogging()
}

