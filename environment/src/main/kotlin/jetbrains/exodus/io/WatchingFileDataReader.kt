/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.io

import com.sun.nio.file.SensitivityWatchEventModifier
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.tryUpdate
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.system.JVMConstants
import mu.KLogging
import java.nio.file.*
import java.util.concurrent.TimeUnit

class WatchingFileDataReader(
    private val envGetter: () -> EnvironmentImpl?,
    internal val fileDataReader: FileDataReader
) : DataReader {

    companion object : KLogging() {
        private const val DEBOUNCE_INTERVAL = 100L // 100 milliseconds
        private val FORCE_CHECK_INTERVAL =
            java.lang.Long.getLong("jetbrains.exodus.io.watching.forceCheckEach", 3000L) // 3 seconds by default
        private val EVENT_KINDS =
            arrayOf(StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE)
    }

    private val watchService = FileSystems.getDefault().newWatchService()
    private val watchKey = fileDataReader.dir.toPath().let {
        if (JVMConstants.IS_MAC) {
            it.register(watchService, EVENT_KINDS, SensitivityWatchEventModifier.HIGH)
        } else {
            it.register(watchService, EVENT_KINDS)
        }
    }
    private val newDataListeners: MutableSet<(prevHighAddress: Long, newHighAddress: Long) -> Unit> = linkedSetOf()

    @Volatile
    private var stopped = false

    init {
        Thread { doWatch() }.apply { name = "Xodus watcher for ${fileDataReader.dir}" }.start()
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

    /**
     * Add callback that is invoked when new data arrives and Environment is successfully updated.
     */
    fun addNewDataListener(listener: (Long, Long) -> Unit) =
        synchronized(newDataListeners) {
            newDataListeners.add(listener)
        }

    @Suppress("unused")
    fun removeNewDataListener(listener: (Long, Long) -> Unit) =
        synchronized(newDataListeners) {
            newDataListeners.remove(listener)
        }

    private fun doWatch() {
        val currentThread = Thread.currentThread()
        var lastUpdated = 0L
        while (!stopped) {
            try {
                val watchKey: WatchKey?
                var hasFileUpdates = false
                try {
                    watchKey = watchService.poll(100, TimeUnit.MILLISECONDS)
                    val events = watchKey?.pollEvents()
                    if (events == null || events.isEmpty()) {
                        if (System.currentTimeMillis() - lastUpdated > FORCE_CHECK_INTERVAL) {
                            lastUpdated = doUpdate(true, currentThread)
                        }
                        continue
                    }
                    for (event in events) {
                        val eventContext = event.context()
                        if (eventContext is Path && LogUtil.LOG_FILE_NAME_FILTER.accept(
                                null,
                                eventContext.fileName.toString()
                            )
                        ) {
                            hasFileUpdates = true
                            break
                        }
                    }
                } catch (e: InterruptedException) {
                    logger.warn(e) { "File watcher interrupted" }
                    currentThread.interrupt()
                    return
                } catch (ignore: ClosedWatchServiceException) {
                    return
                }
                if (lastUpdated > 0) {
                    val debounce = DEBOUNCE_INTERVAL + (lastUpdated - System.currentTimeMillis())
                    if (debounce > 5) {
                        try {
                            Thread.sleep(debounce)
                        } catch (e: InterruptedException) {
                            currentThread.interrupt()
                            return
                        }
                    }
                }
                if (hasFileUpdates) {
                    lastUpdated = doUpdate(false, currentThread)
                }
                if (!watchKey.reset()) {
                    logger.info { "Watch service is no longer valid for ${currentThread.name}, exiting..." }
                    return
                }
            } catch (t: Throwable) {
                if (!stopped) {
                    logger.error(t) { currentThread.name }
                }
            }
        }
    }

    // returns
    private fun doUpdate(force: Boolean, currentThread: Thread): Long {
        envGetter()?.run {
            val prevHighAddress = log.highAddress
            if (!tryUpdate()) {
                logger.debug { (if (force) "Can't force-update env at " else "Can't update env at ") + location }
            } else {
                logger.debug { (if (force) "Env force-updated at " else "Env updated at ") + location }
                val newHighAddress = log.highAddress
                if (newHighAddress > prevHighAddress) {
                    synchronized(newDataListeners) {
                        newDataListeners.toTypedArray()
                    }.forEach { listener ->
                        try {
                            listener(prevHighAddress, newHighAddress)
                        } catch (t: Throwable) {
                            logger.error(t) { "New data listener failed for ${currentThread.name}" }
                        }
                    }
                }
            }
        }
        return System.currentTimeMillis()
    }
}