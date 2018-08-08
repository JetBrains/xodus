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
package jetbrains.exodus.io

import com.sun.nio.file.SensitivityWatchEventModifier
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.tryUpdate
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.system.JVMConstants
import mu.KLogging
import java.nio.file.*
import java.util.concurrent.TimeUnit

class WatchingFileDataReader(private val envGetter: () -> EnvironmentImpl?, internal val fileDataReader: FileDataReader) : DataReader {

    companion object : KLogging() {
        private const val IDLE_FORCE_CHECK_INTERVAL = 3000L // 3 seconds
        private const val DEBOUNCE_INTERVAL = 100L // 100 milliseconds
    }

    private val watchService = FileSystems.getDefault().newWatchService()
    private val watchKey = fileDataReader.dir.toPath().let {
        if (JVMConstants.IS_MAC) {
            it.register(watchService,
                    arrayOf(StandardWatchEventKinds.ENTRY_MODIFY),
                    SensitivityWatchEventModifier.HIGH)
        } else {
            it.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
        }
    }

    @Volatile
    private var stopped = false

    init {
        Thread(Runnable { doWatch() }).start()
    }

    override fun getLocation() = fileDataReader.location

    override fun getBlocks() = fileDataReader.blocks

    override fun getBlocks(fromAddress: Long) = fileDataReader.getBlocks(fromAddress)

    override fun close() {
        stopped = true
        watchKey.cancel()
        watchService.close()
        fileDataReader.close()
    }

    private fun doWatch() {
        var lastDirty = Long.MIN_VALUE
        while (!stopped) {
            val watchKey: WatchKey?
            var hasFileUpdates = false
            try {
                watchKey = watchService.poll(45, TimeUnit.MILLISECONDS)
                val events = watchKey?.pollEvents()
                if (events == null || events.isEmpty()) {
                    if (lastDirty > Long.MIN_VALUE && System.currentTimeMillis() - lastDirty > IDLE_FORCE_CHECK_INTERVAL) {
                        lastDirty = doUpdate(true)
                    }
                    continue
                }
                for (event in events) {
                    val eventContext = event.context()
                    if (eventContext is Path && LogUtil.LOG_FILE_NAME_FILTER.accept(null, eventContext.fileName.toString())) {
                        hasFileUpdates = true
                        break
                    }
                }
            } catch (e: InterruptedException) {
                logger.warn(e) { "File watcher interrupted" }
                Thread.currentThread().interrupt()
                return
            } catch (ignore: ClosedWatchServiceException) {
                return
            }

            if (lastDirty > Long.MIN_VALUE) {
                val debounce = DEBOUNCE_INTERVAL + (lastDirty - System.currentTimeMillis())
                if (debounce > 5) {
                    try {
                        Thread.sleep(debounce)
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        return
                    }

                }
            }
            if (hasFileUpdates) {
                lastDirty = doUpdate(false)
            }
            if (!watchKey.reset()) {
                return
            }
        }
    }

    private fun doUpdate(force: Boolean): Long {
        val env = envGetter()
        if (env != null) {
            if (env.tryUpdate()) {
                logger.info { (if (force) "Env force-updated at " else "Env updated at ") + env.location }
                return Long.MIN_VALUE
            } else {
                logger.info { (if (force) "Can't force-update env at " else "Can't update env at ") + env.location }
            }
        }
        return System.currentTimeMillis()
    }
}