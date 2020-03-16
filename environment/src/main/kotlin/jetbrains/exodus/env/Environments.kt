/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.env

import jetbrains.exodus.ExodusException
import jetbrains.exodus.crypto.newCipherProvider
import jetbrains.exodus.io.FileDataWriter
import jetbrains.exodus.io.SharedOpenFilesCache
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.LogUtil
import java.io.File

object Environments {

    @JvmStatic
    fun newInstance(dir: String, subDir: String, ec: EnvironmentConfig): Environment = prepare { EnvironmentImpl(newLogInstance(File(dir, subDir), ec), ec) }

    @JvmStatic
    fun newInstance(dir: String): Environment = newInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newInstance(log: Log, ec: EnvironmentConfig): Environment = prepare { EnvironmentImpl(log, ec) }

    @JvmStatic
    fun newInstance(dir: String, ec: EnvironmentConfig): Environment = prepare { EnvironmentImpl(newLogInstance(File(dir), ec), ec) }

    @JvmStatic
    fun newInstance(dir: File): Environment = newInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newInstance(dir: File, ec: EnvironmentConfig): Environment = prepare { EnvironmentImpl(newLogInstance(dir, ec), ec) }

    @JvmStatic
    fun newInstance(config: LogConfig): Environment = newInstance(config, EnvironmentConfig())

    @JvmStatic
    fun newInstance(config: LogConfig, ec: EnvironmentConfig): Environment = prepare { EnvironmentImpl(newLogInstance(config, ec), ec) }

    @JvmStatic
    fun newContextualInstance(dir: String, subDir: String, ec: EnvironmentConfig): ContextualEnvironment =
            prepare { ContextualEnvironmentImpl(newLogInstance(File(dir, subDir), ec), ec) }

    @JvmStatic
    fun newContextualInstance(dir: String): ContextualEnvironment = newContextualInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newContextualInstance(dir: String, ec: EnvironmentConfig): ContextualEnvironment = prepare { ContextualEnvironmentImpl(newLogInstance(File(dir), ec), ec) }

    @JvmStatic
    fun newContextualInstance(dir: File): ContextualEnvironment = newContextualInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newContextualInstance(dir: File, ec: EnvironmentConfig): ContextualEnvironment = prepare { ContextualEnvironmentImpl(newLogInstance(dir, ec), ec) }

    @JvmStatic
    fun newContextualInstance(config: LogConfig, ec: EnvironmentConfig): ContextualEnvironment = prepare { ContextualEnvironmentImpl(newLogInstance(config, ec), ec) }

    @JvmStatic
    fun newLogInstance(dir: File, ec: EnvironmentConfig): Log = newLogInstance(LogConfig().setLocation(dir.path), ec)

    @JvmStatic
    fun newLogInstance(config: LogConfig, ec: EnvironmentConfig): Log {
        return newLogInstance(config.apply {
            val maxMemory = ec.memoryUsage
            if (maxMemory != null) {
                memoryUsage = maxMemory
            } else {
                memoryUsagePercentage = ec.memoryUsagePercentage
            }
            setReaderWriterProvider(ec.logDataReaderWriterProvider)
            if (config.readerWriterProvider?.isReadonly == true) {
                ec.envIsReadonly = true
                config.isLockIgnored = true
            }
            fileSize = ec.logFileSize
            lockTimeout = ec.logLockTimeout
            cachePageSize = ec.logCachePageSize
            cacheOpenFilesCount = ec.logCacheOpenFilesCount
            isDurableWrite = ec.logDurableWrite
            isSharedCache = ec.isLogCacheShared
            isNonBlockingCache = ec.isLogCacheNonBlocking
            cacheGenerationCount = ec.logCacheGenerationCount
            isCleanDirectoryExpected = ec.isLogCleanDirectoryExpected
            isClearInvalidLog = ec.isLogClearInvalid
            syncPeriod = ec.logSyncPeriod
            isFullFileReadonly = ec.isLogFullFileReadonly
            cipherProvider = ec.cipherId?.let { cipherId -> newCipherProvider(cipherId) }
            cipherKey = ec.cipherKey
            cipherBasicIV = ec.cipherBasicIV
        })
    }

    @JvmStatic
    fun newLogInstance(config: LogConfig): Log = Log(config.also { SharedOpenFilesCache.setSize(config.cacheOpenFilesCount) })

    private fun <T : EnvironmentImpl> prepare(envCreator: () -> T): T {
        var env = envCreator()
        val ec = env.environmentConfig
        if (ec.envCompactOnOpen) {
            val location = env.location
            File(location, "compactTemp${System.currentTimeMillis()}").let { tempDir ->
                if (!tempDir.mkdir()) {
                    EnvironmentImpl.loggerError("Failed to create temporary directory: $tempDir")
                    return@let
                }
                if (tempDir.freeSpace < env.diskUsage) {
                    EnvironmentImpl.loggerError("Not enough free disk space to compact the database: $location")
                    tempDir.delete()
                    return@let
                }
                env.copyTo(tempDir, false, null) { msg ->
                    EnvironmentImpl.loggerInfo(msg.toString())
                }
                val files = env.log.allFileAddresses
                env.close()
                files.forEach { fileAddress ->
                    val file = File(location, LogUtil.getLogFilename(fileAddress))
                    if (!FileDataWriter.renameFile(file)) {
                        EnvironmentImpl.loggerError("Failed to reanme file: $file")
                        return@let
                    }
                }
                LogUtil.listFiles(tempDir).forEach { file ->
                    if (!file.renameTo(File(location, file.name))) {
                        throw ExodusException("Failed to reanme file: $file")
                    }
                }
                env = envCreator()
            }
        }
        env.gc.utilizationProfile.load()
        val metaServer = ec.metaServer
        metaServer?.start(env)
        return env
    }
}