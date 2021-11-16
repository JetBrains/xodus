/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.log

import jetbrains.exodus.core.execution.RunnableJob
import jetbrains.exodus.core.execution.executeIterable
import jetbrains.exodus.util.DeferredIO
import java.lang.Integer.max

/**
 * Populates LogCache with file pages of this Log in historical order.
 */
internal fun Log.warmup() {
    // do warmup asynchronously
    val processor = DeferredIO.getJobProcessor()
    processor.queue(RunnableJob {
        // number of files to walk through at maximum
        val maxFiles = cache.memoryUsage / fileLengthBound
        val files = allFileAddresses.take(max(1, maxFiles.toInt())).reversed()
        val size = files.size
        val it = DataIterator(this)
        val pageSize = config.cachePageSize
        Log.logger.info("Warming LogCache up with newest $size ${if (size > 1) "files" else "file"} at $location")
        processor.executeIterable(files) { address ->
            Log.logger.info("Warming up ${LogUtil.getLogFilename(address)}")
            var pageAddress = address
            while (pageAddress < address + fileLengthBound && pageAddress + pageSize < highAddress) {
                it.checkPage(pageAddress)
                pageAddress += pageSize
            }
        }
    })
}