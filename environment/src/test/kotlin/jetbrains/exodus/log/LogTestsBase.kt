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
package jetbrains.exodus.log

import jetbrains.exodus.*
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.io.*
import jetbrains.exodus.io.SharedOpenFilesCache.Companion.setSize
import jetbrains.exodus.log.LogConfig.Companion.create
import jetbrains.exodus.util.IOUtil.deleteFile
import jetbrains.exodus.util.IOUtil.deleteRecursively
import org.junit.After
import org.junit.Before
import java.io.*

open class LogTestsBase {
    @Volatile
    private var _log: Log? = null
    protected val log: Log
        get() {
            if (_log == null) {
                synchronized(this) {
                    if (_log == null) {
                        Log.invalidateSharedCache()
                        _log = Log(create(reader!!, writer!!), EnvironmentImpl.CURRENT_FORMAT_VERSION)
                    }
                }
            }

        return _log!!
    }

    private var _logDirectory: File? = null
    protected val logDirectory: File
        get() {
            if (_logDirectory == null) {
                _logDirectory = TestUtil.createTempDir()
            }
            return _logDirectory!!
        }

    protected var reader: DataReader? = null
    protected var writer: DataWriter? = null

    protected fun clearLog() {
        _log = null
    }

    @Before
    @Throws(IOException::class)
    fun setUp() {
        setSize(16)
        val testsDirectory = _logDirectory

        if (testsDirectory != null) {
            if (testsDirectory.exists()) {
                deleteRecursively(testsDirectory)
            } else if (!testsDirectory.mkdir()) {
                throw IOException("Failed to create directory for tests.")
            }
        }

        synchronized(this) {
            val logRW = createLogRW()
            reader = logRW.getFirst()
            writer = logRW.getSecond()
        }
    }

    @After
    fun tearDown() {
        closeLog()
        val testsDirectory = _logDirectory
        if (testsDirectory != null) {
            deleteRecursively(testsDirectory)
            deleteFile(testsDirectory)
            _logDirectory = null
        }
    }

    protected open fun createLogRW(): Pair<DataReader, DataWriter> {
        val reader = FileDataReader(logDirectory)
        return Pair(reader, AsyncFileDataWriter(reader))
    }

    fun initLog(fileSize: Long, cachePageSize: Int) {
        initLog(LogConfig().setFileSize(fileSize).setCachePageSize(cachePageSize))
    }

    private fun initLog(config: LogConfig) {
        if (_log == null) {
            synchronized(this) {
                if (_log == null) {
                    Log.invalidateSharedCache()
                    _log = Log(config.setReaderWriter(reader!!, writer!!), EnvironmentImpl.CURRENT_FORMAT_VERSION)
                }
            }
        }
    }

    fun closeLog() {
        if (_log != null) {
            _log!!.close()
            _log = null
        }
    }

    companion object {
        fun createOneKbLoggable(): TestLoggable {
            return TestLoggable(126.toByte(), ArrayByteIterable(ByteArray(1024), 1024), Loggable.NO_STRUCTURE_ID)
        }
    }
}
